/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.coders

import java.io.{InputStream, OutputStream}

import com.spotify.scio.coders.instances.Implicits
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{AtomicCoder, Coder => BCoder}
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.apache.beam.sdk.values.KV

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
@implicitNotFound(
  """
Cannot find an implicit Coder instance for type:

  >> ${T}

  This can happen for a few reasons, but the most common case is that a data
  member somewhere within this type doesn't have an implicit Coder instance in scope.

  Here are some debugging hints:
    - For Option types, ensure that a Coder instance is in scope for the non-Option version.
    - For List and Seq types, ensure that a Coder instance is in scope for a single element.
    - For generic methods, you may need to add an implicit parameter so that
        def foo[T](coll: SCollection[SomeClass], param: String): SCollection[T]

      may become:
        def foo[T](coll: SCollection[SomeClass],
                   param: String)(implicit c: Coder[T]): SCollection[T]
                                                ^
      Alternatively, you can use a context bound instead of an implicit parameter:
        def foo[T: Coder](coll: SCollection[SomeClass], param: String): SCollection[T]
                    ^
      read more here: https://spotify.github.io/scio/migrations/v0.7.0-Migration-Guide#add-missing-context-bounds

    - You can check that an instance exists for Coder in the REPL or in your code:
        scala> com.spotify.scio.coders.Coder[Foo]
      And find the missing instance and construct it as needed.
"""
)
sealed trait Coder[T] extends Serializable

private[scio] final class Ref[T](val typeName: String, c: => Coder[T]) extends Coder[T] {
  def value: Coder[T] = c

  override def toString(): String = s"""Ref($typeName)"""
}

private[scio] object Ref {
  def apply[T](t: String, c: => Coder[T]): Ref[T] = new Ref[T](t, c)
  def unapply[T](c: Ref[T]): Option[(String, Coder[T])] = Option((c.typeName, c.value))
}

final case class Beam[T] private (beam: BCoder[T]) extends Coder[T] {
  override def toString: String = s"Beam($beam)"
}
final case class Fallback[T] private (ct: ClassTag[T]) extends Coder[T] {
  override def toString: String = s"Fallback($ct)"
}
final case class Transform[A, B] private (c: Coder[A], f: BCoder[A] => Coder[B]) extends Coder[B] {
  override def toString: String = s"Transform($c, $f)"
}
final case class Disjunction[T, Id] private (
  typeName: String,
  idCoder: Coder[Id],
  id: T => Id,
  coder: Map[Id, Coder[T]]
) extends Coder[T] {
  override def toString: String = s"Disjunction($typeName, $coder)"
}

final case class Record[T] private (
  typeName: String,
  cs: Array[(String, Coder[Any])],
  construct: Seq[Any] => T,
  destruct: T => Array[Any]
) extends Coder[T] {
  override def toString: String = {
    val str = cs
      .map {
        case (k, v) => s"($k, $v)"
      }
    s"Record($typeName, ${str.mkString(", ")})"
  }
}

// KV are special in beam and need to be serialized using an instance of KvCoder.
final case class KVCoder[K, V] private (koder: Coder[K], voder: Coder[V]) extends Coder[KV[K, V]] {
  override def toString: String = s"KVCoder($koder, $voder)"
}

private final case class DisjunctionCoder[T, Id](
  typeName: String,
  idCoder: BCoder[Id],
  id: T => Id,
  coders: Map[Id, BCoder[T]]
) extends AtomicCoder[T] {
  def encode(value: T, os: OutputStream): Unit = {
    val i = id(value)
    idCoder.encode(i, os)
    coders(i).encode(value, os)
  }

  def decode(is: InputStream): T = {
    val i = idCoder.decode(is)
    coders(i).decode(is)
  }

  override def verifyDeterministic(): Unit = {
    def verify(label: String, c: BCoder[_]): List[(String, NonDeterministicException)] = {
      try {
        c.verifyDeterministic()
        Nil
      } catch {
        case e: NonDeterministicException =>
          val reason = s"case $label is using non-deterministic $c"
          List(reason -> e)
      }
    }

    val problems =
      coders.toList.flatMap { case (id, c) => verify(id.toString, c) } ++
        verify("id", idCoder)

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def toString: String = {
    val parts = s"id -> $idCoder" :: coders.map { case (id, coder) => s"$id -> $coder" }.toList
    val body = parts.mkString(", ")

    s"DisjunctionCoder[$typeName]($body)"
  }

  override def consistentWithEquals(): Boolean =
    coders.values.forall(_.consistentWithEquals())

  override def structuralValue(value: T): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      coders(id(value)).structuralValue(value)
    }
}

private[scio] final case class RefCoder[T](val typeName: String, var coder: BCoder[T])
    extends BCoder[T] {
  def setImpl(_c: BCoder[T]): Unit = coder = _c

  private def check[A](t: => A): A = {
    require(coder != null, s"Coder implementation should not be null in ${this}")
    t
  }

  def decode(inStream: InputStream): T = check { coder.decode(inStream) }
  def encode(value: T, outStream: OutputStream): Unit = check { coder.encode(value, outStream) }
  def getCoderArguments(): java.util.List[_ <: BCoder[_]] = check { coder.getCoderArguments() }
  def verifyDeterministic(): Unit = check { coder.verifyDeterministic() }

  override def consistentWithEquals(): Boolean = check { coder.consistentWithEquals() }
  override def structuralValue(value: T): AnyRef = check {
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      coder.structuralValue(value)
    }
  }

  override def toString(): String =
    s"RefCoder($typeName, ${if (coder == null) "null" else "<coder ref>"})"
}

// XXX: Workaround a NPE deep down the stack in Beam
// info]   java.lang.NullPointerException: null value in entry: T=null
private[scio] case class WrappedBCoder[T](u: BCoder[T]) extends BCoder[T] {
  /**
   * Eagerly compute a stack trace on materialization
   * to provide a helpful stacktrace if an exception happens
   */
  private[this] val materializationStackTrace: Array[StackTraceElement] =
    CoderStackTrace.prepare

  override def toString: String = u.toString

  @inline private def catching[A](a: => A) =
    try {
      a
    } catch {
      case ex: Throwable =>
        // prior to scio 0.8, a wrapped exception was thrown. It is no longer the case, as some
        // backends (e.g. Flink) use exceptions as a way to signal from the Coder to the layers
        // above here; we therefore must alter the type of exceptions passing through this block.
        throw CoderStackTrace.append(ex, None, materializationStackTrace)
    }

  override def encode(value: T, os: OutputStream): Unit =
    catching { u.encode(value, os) }

  override def decode(is: InputStream): T =
    catching { u.decode(is) }

  override def getCoderArguments: java.util.List[_ <: BCoder[_]] = u.getCoderArguments

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit = u.verifyDeterministic()
  override def consistentWithEquals(): Boolean = u.consistentWithEquals()
  override def structuralValue(value: T): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      u.structuralValue(value)
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: T): Boolean =
    u.isRegisterByteSizeObserverCheap(value)
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit =
    u.registerByteSizeObserver(value, observer)
}

private[scio] object WrappedBCoder {
  def create[T](u: BCoder[T]): BCoder[T] =
    u match {
      case WrappedBCoder(_) => u
      case _                => new WrappedBCoder(u)
    }
}

// Coder used internally specifically for Magnolia derived coders.
// It's technically possible to define Product coders only in terms of `Coder.transform`
// This is just faster
private[scio] final case class RecordCoder[T](
  typeName: String,
  cs: Array[(String, BCoder[Any])],
  construct: Seq[Any] => T,
  destruct: T => Array[Any]
) extends AtomicCoder[T] {
  @inline def onErrorMsg[A](msg: => String)(f: => A): A =
    try {
      f
    } catch {
      case e: Exception =>
        throw new RuntimeException(msg, e)
    }

  override def encode(value: T, os: OutputStream): Unit = {
    var i = 0
    val array = destruct(value)
    while (i < array.length) {
      val (label, c) = cs(i)
      val v = array(i)
      onErrorMsg(
        s"Exception while trying to `encode` an instance of $typeName:  Can't encode field $label value $v"
      ) {
        c.encode(v, os)
      }
      i += 1
    }
  }

  override def decode(is: InputStream): T = {
    val vs = new Array[Any](cs.length)
    var i = 0
    while (i < cs.length) {
      val (label, c) = cs(i)
      onErrorMsg(
        s"Exception while trying to `decode` an instance of $typeName: Can't decode field $label"
      ) {
        vs.update(i, c.decode(is))
      }
      i += 1
    }
    construct(vs.toSeq)
  }

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val problems = cs.toList.flatMap {
      case (label, c) =>
        try {
          c.verifyDeterministic()
          Nil
        } catch {
          case e: NonDeterministicException =>
            val reason = s"field $label is using non-deterministic $c"
            List(reason -> e)
        }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def toString: String = {
    val body = cs.map { case (label, c) => s"$label -> $c" }.mkString(", ")
    s"RecordCoder[$typeName]($body)"
  }

  override def consistentWithEquals(): Boolean = cs.forall(_._2.consistentWithEquals())
  override def structuralValue(value: T): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      val b = Seq.newBuilder[AnyRef]
      var i = 0
      val array = destruct(value)
      while (i < cs.length) {
        val (label, c) = cs(i)
        val v = array(i)
        onErrorMsg(s"Exception while trying to `encode` field $label with value $v") {
          b += c.structuralValue(v)
        }
        i += 1
      }
      b.result()
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: T): Boolean = {
    var res = true
    var i = 0
    val array = destruct(value)
    while (res && i < cs.length) {
      res = cs(i)._2.isRegisterByteSizeObserverCheap(array(i))
      i += 1
    }
    res
  }
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit = {
    var i = 0
    val array = destruct(value)
    while (i < cs.length) {
      val (_, c) = cs(i)
      val v = array(i)
      c.registerByteSizeObserver(v, observer)
      i += 1
    }
  }
}

/**
 * Coder Grammar is used to explicitly specify Coder derivation for types used in pipelines.
 *
 * The CoderGrammar can be used as follows:
 * - To find the Coder being implicitly derived by Scio. (Debugging)
 *   {{{
 *     def c: Coder[MyType] = Coder[MyType]
 *   }}}
 *
 * - To generate an implicit instance to be in scope for type T, use [[Coder.gen]]
 *   {{{
 *     implicit def coderT: Coder[T] = Coder.gen[T]
 *   }}}
 *
 *   Note: Implicit Coders for all parameters of the constructor of type T should be in scope for
 *         [[Coder.gen]] to be able to derive the Coder.
 *
 * - To define a Coder of custom type, where the type can be mapped to some other type for which
 *   a Coder is known, use [[Coder.xmap]]
 *
 * - To explicitly use kryo Coder use [[Coder.kryo]]
 *
 */
sealed trait CoderGrammar {
  /**
   * Create a ScioCoder from a Beam Coder
   */
  def beam[T](beam: BCoder[T]): Coder[T] =
    Beam(beam)
  def kv[K, V](koder: Coder[K], voder: Coder[V]): Coder[KV[K, V]] =
    KVCoder(koder, voder)

  /**
   * Create an instance of Kryo Coder for a given Type.
   *
   * Eg:
   *   A kryo Coder for [[org.joda.time.Interval]] would look like:
   *   {{{
   *     implicit def jiKryo: Coder[Interval] = Coder.kryo[Interval]
   *   }}}
   */
  def kryo[T](implicit ct: ClassTag[T]): Coder[T] =
    Fallback[T](ct)
  def transform[A, B](c: Coder[A])(f: BCoder[A] => Coder[B]): Coder[B] =
    Transform(c, f)
  def disjunction[T, Id: Coder](typeName: String, coder: Map[Id, Coder[T]])(id: T => Id): Coder[T] =
    Disjunction(typeName, Coder[Id], id, coder)

  /**
   * Given a Coder[A], create a Coder[B] by defining two functions A => B and B => A.
   * The Coder[A] can be resolved implicitly by calling Coder[A]
   *
   * Eg: Coder for [[org.joda.time.Interval]] can be defined by having the following implicit in
   *     scope. Without this implicit in scope Coder derivation falls back to Kryo.
   *     {{{
   *       implicit def jiCoder: Coder[Interval] =
   *         Coder.xmap(Coder[(Long, Long)])(t => new Interval(t._1, t._2),
   *            i => (i.getStartMillis, i.getEndMillis))
   *     }}}
   *     In the above example we implicitly derive Coder[(Long, Long)] and we define two functions,
   *     one to convert a tuple (Long, Long) to Interval, and a second one to convert an Interval
   *     to a tuple of (Long, Long)
   */
  def xmap[A, B](c: Coder[A])(f: A => B, t: B => A): Coder[B] = {
    @inline def toB(bc: BCoder[A]) = new AtomicCoder[B] {
      override def encode(value: B, os: OutputStream): Unit =
        bc.encode(t(value), os)
      override def decode(is: InputStream): B =
        f(bc.decode(is))

      // delegate methods for determinism and equality checks
      override def verifyDeterministic(): Unit = bc.verifyDeterministic()
      override def consistentWithEquals(): Boolean = bc.consistentWithEquals()
      override def structuralValue(value: B): AnyRef =
        if (consistentWithEquals()) {
          value.asInstanceOf[AnyRef]
        } else {
          bc.structuralValue(t(value))
        }

      // delegate methods for byte size estimation
      override def isRegisterByteSizeObserverCheap(value: B): Boolean =
        bc.isRegisterByteSizeObserverCheap(t(value))
      override def registerByteSizeObserver(value: B, observer: ElementByteSizeObserver): Unit =
        bc.registerByteSizeObserver(t(value), observer)
    }
    Transform[A, B](c, bc => Coder.beam(toB(bc)))
  }

  private[scio] def record[T](
    typeName: String,
    cs: Array[(String, Coder[Any])],
    construct: Seq[Any] => T,
    destruct: T => Array[Any]
  ): Coder[T] =
    Record[T](typeName, cs, construct, destruct)
}

import java.io.{InputStream, OutputStream}
import java.util
import java.util.Collections

import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder, _}
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.util.common.ElementByteSizeObserver

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.collection.{BitSet, SortedSet, TraversableOnce, mutable => m}
import scala.collection.convert.Wrappers
import scala.util.Try

object Coder extends CoderGrammar with Implicits {
  @inline final def apply[T](implicit c: Coder[T]): Coder[T] = c

  implicit def charCoder: Coder[Char] =
    Coder.xmap(Coder.beam(ByteCoder.of()))(_.toChar, _.toByte)
  implicit def byteCoder: Coder[Byte] =
    Coder.beam(ByteCoder.of().asInstanceOf[BCoder[Byte]])
  implicit def stringCoder: Coder[String] =
    Coder.beam(StringUtf8Coder.of())
  implicit def shortCoder: Coder[Short] =
    Coder.beam(BigEndianShortCoder.of().asInstanceOf[BCoder[Short]])
  implicit def intCoder: Coder[Int] =
    Coder.beam(VarIntCoder.of().asInstanceOf[BCoder[Int]])
  implicit def longCoder: Coder[Long] =
    Coder.beam(BigEndianLongCoder.of().asInstanceOf[BCoder[Long]])
  implicit def floatCoder: Coder[Float] = Coder.beam(SFloatCoder)
  implicit def doubleCoder: Coder[Double] = Coder.beam(SDoubleCoder)

  implicit def booleanCoder: Coder[Boolean] =
    Coder.beam(BooleanCoder.of().asInstanceOf[BCoder[Boolean]])
  implicit def unitCoder: Coder[Unit] = Coder.beam(UnitCoder)
  implicit def nothingCoder: Coder[Nothing] = Coder.beam[Nothing](NothingCoder)

  implicit def bigIntCoder: Coder[BigInt] =
    Coder.xmap(Coder.beam(BigIntegerCoder.of()))(BigInt.apply, _.bigInteger)

  implicit def bigDecimalCoder: Coder[BigDecimal] =
    Coder.xmap(Coder.beam(BigDecimalCoder.of()))(BigDecimal.apply, _.bigDecimal)

  implicit def tryCoder[A: Coder]: Coder[Try[A]] =
    Coder.gen[Try[A]]

  implicit def eitherCoder[A: Coder, B: Coder]: Coder[Either[A, B]] =
    Coder.gen[Either[A, B]]

  implicit def optionCoder[T, S[_] <: Option[_]](implicit c: Coder[T]): Coder[S[T]] =
    Coder
      .transform(c) { bc =>
        Coder.beam(new OptionCoder[T](bc))
      }
      .asInstanceOf[Coder[S[T]]]

  implicit def noneCoder: Coder[None.type] =
    optionCoder[Nothing, Option](nothingCoder).asInstanceOf[Coder[None.type]]

  implicit def bitSetCoder: Coder[BitSet] = Coder.beam(new BitSetCoder)

  implicit def seqCoder[T: Coder]: Coder[Seq[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new SeqCoder[T](bc))
    }

  import shapeless.Strict
  implicit def pairCoder[A, B](implicit CA: Strict[Coder[A]], CB: Strict[Coder[B]]): Coder[(A, B)] =
    Coder.transform(CA.value) { ac =>
      Coder.transform(CB.value) { bc =>
        Coder.beam(new PairCoder[A, B](ac, bc))
      }
    }

  // TODO: proper chunking implementation
  implicit def iterableCoder[T: Coder]: Coder[Iterable[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new IterableCoder[T](bc))
    }

  implicit def throwableCoder[T <: Throwable: ClassTag]: Coder[T] =
    Coder.kryo[T]

  // specialized coder. Since `::` is a case class, Magnolia would derive an incorrect one...
  implicit def listCoder[T: Coder]: Coder[List[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new ListCoder[T](bc))
    }

  implicit def traversableOnceCoder[T: Coder]: Coder[TraversableOnce[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new TraversableOnceCoder[T](bc))
    }

  implicit def setCoder[T: Coder]: Coder[Set[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new SetCoder[T](bc))
    }

  implicit def mutableSetCoder[T: Coder]: Coder[m.Set[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new MutableSetCoder[T](bc))
    }

  implicit def vectorCoder[T: Coder]: Coder[Vector[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new VectorCoder[T](bc))
    }

  implicit def arrayBufferCoder[T: Coder]: Coder[m.ArrayBuffer[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new ArrayBufferCoder[T](bc))
    }

  implicit def bufferCoder[T: Coder]: Coder[m.Buffer[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new BufferCoder[T](bc))
    }

  implicit def listBufferCoder[T: Coder]: Coder[m.ListBuffer[T]] =
    Coder.xmap(bufferCoder[T])(m.ListBuffer(_: _*), identity)

  implicit def arrayCoder[T: Coder: ClassTag]: Coder[Array[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new ArrayCoder[T](bc))
    }

  implicit def arrayByteCoder: Coder[Array[Byte]] =
    Coder.beam(ByteArrayCoder.of())

  implicit def wrappedArrayCoder[T: Coder: ClassTag](
    implicit wrap: Array[T] => m.WrappedArray[T]
  ): Coder[m.WrappedArray[T]] =
    Coder.xmap(Coder[Array[T]])(wrap, _.array)

  implicit def mutableMapCoder[K: Coder, V: Coder]: Coder[m.Map[K, V]] =
    Coder.transform(Coder[K]) { kc =>
      Coder.transform(Coder[V]) { vc =>
        Coder.beam(new MutableMapCoder[K, V](kc, vc))
      }
    }

  implicit def mapCoder[K: Coder, V: Coder]: Coder[Map[K, V]] =
    Coder.transform(Coder[K]) { kc =>
      Coder.transform(Coder[V]) { vc =>
        Coder.beam(new MapCoder[K, V](kc, vc))
      }
    }

  implicit def sortedSetCoder[T: Coder: Ordering]: Coder[SortedSet[T]] =
    Coder.transform(Coder[T]) { bc =>
      Coder.beam(new SortedSetCoder[T](bc))
    }
}

private object UnitCoder extends AtomicCoder[Unit] {
  override def encode(value: Unit, os: OutputStream): Unit = ()
  override def decode(is: InputStream): Unit = ()
}

private object NothingCoder extends AtomicCoder[Nothing] {
  override def encode(value: Nothing, os: OutputStream): Unit = ()
  override def decode(is: InputStream): Nothing = ??? // can't possibly happen
}

/**
 * Most Coders TupleX are derived by Magnolia but we specialize Coder[(A, B)] for
 * performance reasons given that pairs are really common and used in groupBy operations.
 */
private final class PairCoder[A, B](ac: BCoder[A], bc: BCoder[B]) extends AtomicCoder[(A, B)] {
  @inline def onErrorMsg[T](msg: => (String, String))(f: => T): T =
    try {
      f
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Exception while trying to `${msg._1}` an instance of Tuple2:" +
            s" Can't decode field ${msg._2}",
          e
        )
    }

  override def encode(value: (A, B), os: OutputStream): Unit = {
    onErrorMsg("encode" -> "_1")(ac.encode(value._1, os))
    onErrorMsg("encode" -> "_2")(bc.encode(value._2, os))
  }
  override def decode(is: InputStream): (A, B) = {
    val _1 = onErrorMsg("decode" -> "_1")(ac.decode(is))
    val _2 = onErrorMsg("decode" -> "_2")(bc.decode(is))
    (_1, _2)
  }

  override def toString: String =
    s"PairCoder(_1 -> $ac, _2 -> $bc)"

  // delegate methods for determinism and equality checks

  override def verifyDeterministic(): Unit = {
    val cs = List("_1" -> ac, "_2" -> bc)
    val problems = cs.toList.flatMap {
      case (label, c) =>
        try {
          c.verifyDeterministic()
          Nil
        } catch {
          case e: NonDeterministicException =>
            val reason = s"field $label is using non-deterministic $c"
            List(reason -> e)
        }
    }

    problems match {
      case (_, e) :: _ =>
        val reasons = problems.map { case (reason, _) => reason }
        throw new NonDeterministicException(this, reasons.asJava, e)
      case Nil =>
    }
  }

  override def consistentWithEquals(): Boolean =
    ac.consistentWithEquals() && bc.consistentWithEquals()

  override def structuralValue(value: (A, B)): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      (ac.structuralValue(value._1), bc.structuralValue(value._2))
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: (A, B)): Boolean =
    ac.isRegisterByteSizeObserverCheap(value._1) && bc.isRegisterByteSizeObserverCheap(value._2)

  override def registerByteSizeObserver(value: (A, B), observer: ElementByteSizeObserver): Unit = {
    ac.registerByteSizeObserver(value._1, observer)
    bc.registerByteSizeObserver(value._2, observer)
  }
}

private abstract class BaseSeqLikeCoder[M[_], T](val elemCoder: BCoder[T])(
  implicit toSeq: M[T] => TraversableOnce[T]
) extends AtomicCoder[M[T]] {
  override def getCoderArguments: java.util.List[_ <: BCoder[_]] =
    Collections.singletonList(elemCoder)

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit = elemCoder.verifyDeterministic()
  override def consistentWithEquals(): Boolean = elemCoder.consistentWithEquals()
  override def structuralValue(value: M[T]): AnyRef =
    if (consistentWithEquals()) {
      value.asInstanceOf[AnyRef]
    } else {
      val b = Seq.newBuilder[AnyRef]
      b.sizeHint(value.size)
      value.foreach(v => b += elemCoder.structuralValue(v))
      b.result()
    }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: M[T]): Boolean = false
  override def registerByteSizeObserver(value: M[T], observer: ElementByteSizeObserver): Unit = {
    if (value.isInstanceOf[Wrappers.JIterableWrapper[_]]) {
      val wrapper = value.asInstanceOf[Wrappers.JIterableWrapper[T]]
      IterableCoder.of(elemCoder).registerByteSizeObserver(wrapper.underlying, observer)
    } else {
      super.registerByteSizeObserver(value, observer)
    }
  }
}

private abstract class SeqLikeCoder[M[_], T](bc: BCoder[T])(
  implicit toSeq: M[T] => TraversableOnce[T]
) extends BaseSeqLikeCoder[M, T](bc) {
  protected val lc = VarIntCoder.of()
  override def encode(value: M[T], outStream: OutputStream): Unit = {
    lc.encode(value.size, outStream)
    value.foreach(bc.encode(_, outStream))
  }
  def decode(inStream: InputStream, builder: m.Builder[T, M[T]]): M[T] = {
    val size = lc.decode(inStream)
    builder.sizeHint(size)
    var i = 0
    while (i < size) {
      builder += bc.decode(inStream)
      i = i + 1
    }
    builder.result()
  }

  override def toString: String =
    s"SeqLikeCoder($bc)"
}

private class OptionCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Option, T](bc) {
  private[this] val bcoder = BooleanCoder.of().asInstanceOf[BCoder[Boolean]]
  override def encode(value: Option[T], os: OutputStream): Unit = {
    bcoder.encode(value.isDefined, os)
    value.foreach { bc.encode(_, os) }
  }

  override def decode(is: InputStream): Option[T] = {
    val isDefined = bcoder.decode(is)
    if (isDefined) Some(bc.decode(is)) else None
  }

  override def toString: String =
    s"OptionCoder($bc)"
}

private class SeqCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Seq, T](bc) {
  override def decode(inStream: InputStream): Seq[T] = decode(inStream, Seq.newBuilder[T])
}

private class ListCoder[T](bc: BCoder[T]) extends SeqLikeCoder[List, T](bc) {
  override def decode(inStream: InputStream): List[T] = decode(inStream, List.newBuilder[T])
}

// TODO: implement chunking
private class TraversableOnceCoder[T](bc: BCoder[T]) extends SeqLikeCoder[TraversableOnce, T](bc) {
  override def decode(inStream: InputStream): TraversableOnce[T] =
    decode(inStream, Seq.newBuilder[T])
}

// TODO: implement chunking
private class IterableCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Iterable, T](bc) {
  override def decode(inStream: InputStream): Iterable[T] =
    decode(inStream, Iterable.newBuilder[T])
}

private class VectorCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Vector, T](bc) {
  override def decode(inStream: InputStream): Vector[T] = decode(inStream, Vector.newBuilder[T])
}

private class ArrayCoder[@specialized(Short, Int, Long, Float, Double, Boolean, Char) T: ClassTag](
  bc: BCoder[T]
) extends SeqLikeCoder[Array, T](bc) {
  override def decode(inStream: InputStream): Array[T] = {
    val size = lc.decode(inStream)
    val arr = new Array[T](size)
    var i = 0
    while (i < size) {
      arr(i) = bc.decode(inStream)
      i += 1
    }
    arr
  }
  override def consistentWithEquals(): Boolean = false
}

private class ArrayBufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.ArrayBuffer, T](bc) {
  override def decode(inStream: InputStream): m.ArrayBuffer[T] =
    decode(inStream, m.ArrayBuffer.newBuilder[T])
}

private class BufferCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.Buffer, T](bc) {
  override def decode(inStream: InputStream): m.Buffer[T] = decode(inStream, m.Buffer.newBuilder[T])
}

private class SetCoder[T](bc: BCoder[T]) extends SeqLikeCoder[Set, T](bc) {
  override def decode(inStream: InputStream): Set[T] = decode(inStream, Set.newBuilder[T])
}

private class MutableSetCoder[T](bc: BCoder[T]) extends SeqLikeCoder[m.Set, T](bc) {
  override def decode(inStream: InputStream): m.Set[T] = decode(inStream, m.Set.newBuilder[T])
}

private class SortedSetCoder[T: Ordering](bc: BCoder[T]) extends SeqLikeCoder[SortedSet, T](bc) {
  override def decode(inStream: InputStream): SortedSet[T] =
    decode(inStream, SortedSet.newBuilder[T])
}

private class BitSetCoder extends AtomicCoder[BitSet] {
  private[this] val lc = VarIntCoder.of()

  def decode(in: InputStream): BitSet = {
    val l = lc.decode(in)
    val builder = BitSet.newBuilder
    builder.sizeHint(l)
    (1 to l).foreach(_ => builder += lc.decode(in))

    builder.result()
  }

  def encode(ts: BitSet, out: OutputStream): Unit = {
    lc.encode(ts.size, out)
    ts.foreach(v => lc.encode(v, out))
  }
}

private class MapCoder[K, V](kc: BCoder[K], vc: BCoder[V]) extends AtomicCoder[Map[K, V]] {
  private[this] val lc = VarIntCoder.of()

  override def encode(value: Map[K, V], os: OutputStream): Unit = {
    lc.encode(value.size, os)
    val it = value.iterator
    while (it.hasNext) {
      val (k, v) = it.next
      kc.encode(k, os)
      vc.encode(v, os)
    }
  }

  override def decode(is: InputStream): Map[K, V] = {
    val l = lc.decode(is)
    val builder = Map.newBuilder[K, V]
    builder.sizeHint(l)
    var i = 0
    while (i < l) {
      val k = kc.decode(is)
      val v = vc.decode(is)
      builder += (k -> v)
      i = i + 1
    }
    builder.result()
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of entries in a Map may be non-deterministic."
    )
  override def consistentWithEquals(): Boolean =
    kc.consistentWithEquals() && vc.consistentWithEquals()
  override def structuralValue(value: Map[K, V]): AnyRef = {
    if (consistentWithEquals()) {
      value
    } else {
      val b = Map.newBuilder[Any, Any]
      b.sizeHint(value.size)
      value.foreach {
        case (k, v) =>
          b += kc.structuralValue(k) -> vc.structuralValue(v)
      }
      b.result()
    }
  }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: Map[K, V]): Boolean = false
  override def registerByteSizeObserver(
    value: Map[K, V],
    observer: ElementByteSizeObserver
  ): Unit = {
    lc.registerByteSizeObserver(value.size, observer)
    value.foreach {
      case (k, v) =>
        kc.registerByteSizeObserver(k, observer)
        vc.registerByteSizeObserver(v, observer)
    }
  }

  override def toString: String =
    s"MapCoder($kc, $vc)"
}

private class MutableMapCoder[K, V](kc: BCoder[K], vc: BCoder[V]) extends AtomicCoder[m.Map[K, V]] {
  private[this] val lc = VarIntCoder.of()

  override def encode(value: m.Map[K, V], os: OutputStream): Unit = {
    lc.encode(value.size, os)
    value.foreach {
      case (k, v) =>
        kc.encode(k, os)
        vc.encode(v, os)
    }
  }

  override def decode(is: InputStream): m.Map[K, V] = {
    val l = lc.decode(is)
    val builder = m.Map.newBuilder[K, V]
    builder.sizeHint(l)
    var i = 0
    while (i < l) {
      val k = kc.decode(is)
      val v = vc.decode(is)
      builder += (k -> v)
      i = i + 1
    }
    builder.result()
  }

  // delegate methods for determinism and equality checks
  override def verifyDeterministic(): Unit =
    throw new NonDeterministicException(
      this,
      "Ordering of entries in a Map may be non-deterministic."
    )
  override def consistentWithEquals(): Boolean =
    kc.consistentWithEquals() && vc.consistentWithEquals()
  override def structuralValue(value: m.Map[K, V]): AnyRef = {
    if (consistentWithEquals()) {
      value
    } else {
      val b = m.Map.newBuilder[Any, Any]
      b.sizeHint(value.size)
      value.foreach {
        case (k, v) =>
          b += kc.structuralValue(k) -> vc.structuralValue(v)
      }
      b.result()
    }
  }

  // delegate methods for byte size estimation
  override def isRegisterByteSizeObserverCheap(value: m.Map[K, V]): Boolean = false
  override def registerByteSizeObserver(
    value: m.Map[K, V],
    observer: ElementByteSizeObserver
  ): Unit = {
    lc.registerByteSizeObserver(value.size, observer)
    value.foreach {
      case (k, v) =>
        kc.registerByteSizeObserver(k, observer)
        vc.registerByteSizeObserver(v, observer)
    }
  }

  override def toString: String =
    s"MutableMapCoder($kc, $vc)"
}

private object SFloatCoder extends BCoder[Float] {
  private val bc = FloatCoder.of()

  override def encode(value: Float, outStream: OutputStream): Unit = bc.encode(value, outStream)
  override def decode(inStream: InputStream): Float = bc.decode(inStream)
  override def getCoderArguments: util.List[_ <: BCoder[_]] = bc.getCoderArguments
  override def verifyDeterministic(): Unit = bc.verifyDeterministic()
  override def structuralValue(value: Float): AnyRef =
    if (value.isNaN) {
      new StructuralByteArray(CoderUtils.encodeToByteArray(bc, value: java.lang.Float))
    } else {
      java.lang.Float.valueOf(value)
    }
  override def toString: String = "FloatCoder"
}

private object SDoubleCoder extends BCoder[Double] {
  private val bc = DoubleCoder.of()

  override def encode(value: Double, outStream: OutputStream): Unit = bc.encode(value, outStream)
  override def decode(inStream: InputStream): Double = bc.decode(inStream)
  override def getCoderArguments: util.List[_ <: BCoder[_]] = bc.getCoderArguments
  override def verifyDeterministic(): Unit = bc.verifyDeterministic()
  override def structuralValue(value: Double): AnyRef =
    if (value.isNaN) {
      new StructuralByteArray(CoderUtils.encodeToByteArray(bc, value: java.lang.Double))
    } else {
      java.lang.Double.valueOf(value)
    }
  override def toString: String = "DoubleCoder"
}

private[coders] object CoderStackTrace {
  val CoderStackElemMarker = new StackTraceElement(
    "### Coder materialization stack ###",
    "",
    "",
    0
  )

  def prepare: Array[StackTraceElement] =
    CoderStackElemMarker +: Thread
      .currentThread()
      .getStackTrace
      .dropWhile(!_.getClassName.contains(CoderMaterializer.getClass.getName))
      .take(10)

  def append[T <: Throwable](
    cause: T,
    additionalMessage: Option[String],
    baseStack: Array[StackTraceElement]
  ): T = {
    cause.printStackTrace()
    val messageItem = additionalMessage.map { msg =>
      new StackTraceElement(s"Due to $msg", "", "", 0)
    }

    val adjustedStack = messageItem ++ cause.getStackTrace ++ baseStack
    cause.setStackTrace(adjustedStack.toArray)
    cause
  }
}
