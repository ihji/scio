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
package com.spotify.scio.schemas

import java.util
import java.util.{List => jList, Map => jMap}

import com.spotify.scio.{FeatureFlag, IsJavaBean, MacroSettings}
import com.spotify.scio.schemas.instances.AllInstances
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.schemas.Schema.FieldType
import org.apache.beam.sdk.schemas.{JavaBeanSchema, SchemaProvider, Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.{Row, TypeDescriptor}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.schemas.Schema.LogicalType

import scala.collection.{mutable, SortedSet}

object Schema extends AllInstances {
  @inline final def apply[T](implicit c: Schema[T]): Schema[T] = c

  final def logicalType[U, T: ClassTag](
    underlying: Type[U]
  )(toBase: T => U, fromBase: U => T): Schema[T] = {
    Type[T](FieldType.logicalType(new LogicalType[T, U] {
      private val clazz = scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]]
      private val className = clazz.getCanonicalName
      override def getIdentifier: String = className
      override def getBaseType: FieldType = underlying.fieldType
      override def toBaseType(input: T): U = toBase(input)
      override def toInputType(base: U): T = fromBase(base)
      override def toString(): String =
        s"LogicalType($className, ${underlying.fieldType.getTypeName()})"
    }))
  }

  implicit val stringSchema: Type[String] =
    Type[String](FieldType.STRING)

  implicit val byteSchema: Type[Byte] =
    Type[Byte](FieldType.BYTE)

  implicit val bytesSchema: Type[Array[Byte]] =
    Type[Array[Byte]](FieldType.BYTES)

  implicit val sortSchema: Type[Short] =
    Type[Short](FieldType.INT16)

  implicit val intSchema: Type[Int] =
    Type[Int](FieldType.INT32)

  implicit val longSchema: Type[Long] =
    Type[Long](FieldType.INT64)

  implicit val floatSchema: Type[Float] =
    Type[Float](FieldType.FLOAT)

  implicit val doubleSchema: Type[Double] =
    Type[Double](FieldType.DOUBLE)

  implicit val bigDecimalSchema: Type[BigDecimal] =
    Type[BigDecimal](FieldType.DECIMAL)

  implicit val booleanSchema: Type[Boolean] =
    Type[Boolean](FieldType.BOOLEAN)

  implicit def optionSchema[T](implicit s: Schema[T]): Schema[Option[T]] =
    OptionType(s)

  implicit def arraySchema[T: ClassTag](implicit s: Schema[T]): Schema[Array[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toArray)

  implicit def listSchema[T](implicit s: Schema[T]): Schema[List[T]] =
    ArrayType(s, _.asJava, _.asScala.toList)

  implicit def seqSchema[T](implicit s: Schema[T]): Schema[Seq[T]] =
    ArrayType(s, _.asJava, _.asScala.toList)

  implicit def traversableOnceSchema[T](implicit s: Schema[T]): Schema[TraversableOnce[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toList)

  implicit def iterableSchema[T](implicit s: Schema[T]): Schema[Iterable[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toList)

  implicit def arrayBufferSchema[T](implicit s: Schema[T]): Schema[mutable.ArrayBuffer[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.ArrayBuffer(xs.asScala: _*))

  implicit def bufferSchema[T](implicit s: Schema[T]): Schema[mutable.Buffer[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.Buffer(xs.asScala: _*))

  implicit def setSchema[T](implicit s: Schema[T]): Schema[Set[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toSet)

  implicit def mutableSetSchema[T](implicit s: Schema[T]): Schema[mutable.Set[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.Set(xs.asScala: _*))

  implicit def sortedSetSchema[T: Ordering](implicit s: Schema[T]): Schema[SortedSet[T]] =
    ArrayType(s, _.toList.asJava, xs => SortedSet(xs.asScala: _*))

  implicit def listBufferSchema[T](implicit s: Schema[T]): Schema[mutable.ListBuffer[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.ListBuffer.apply(xs.asScala: _*))

  implicit def vectorSchema[T](implicit s: Schema[T]): Schema[Vector[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toVector)

  implicit def mapSchema[K, V](implicit k: Schema[K], v: Schema[V]): Schema[Map[K, V]] =
    MapType(k, v, _.asJava, _.asScala.toMap)

  // TODO: WrappedArray ?

  implicit def mutableMapSchema[K, V](
    implicit k: Schema[K],
    v: Schema[V]
  ): Schema[mutable.Map[K, V]] =
    MapType(k, v, _.asJava, _.asScala)

  implicit val jByteSchema: Type[java.lang.Byte] =
    Type[java.lang.Byte](FieldType.BYTE)

  implicit val jBytesSchema: Type[Array[java.lang.Byte]] =
    Type[Array[java.lang.Byte]](FieldType.BYTES)

  implicit val jShortSchema: Type[java.lang.Short] =
    Type[java.lang.Short](FieldType.INT16)

  implicit val jIntegerSchema: Type[java.lang.Integer] =
    Type[java.lang.Integer](FieldType.INT32)

  implicit val jLongSchema: Type[java.lang.Long] =
    Type[java.lang.Long](FieldType.INT64)

  implicit val jFloatSchema: Type[java.lang.Float] =
    Type[java.lang.Float](FieldType.FLOAT)

  implicit val jDoubleSchema: Type[java.lang.Double] =
    Type[java.lang.Double](FieldType.DOUBLE)

  implicit val jBigDecimalSchema: Type[java.math.BigDecimal] =
    Type[java.math.BigDecimal](FieldType.DECIMAL)

  implicit val jBooleanSchema: Type[java.lang.Boolean] =
    Type[java.lang.Boolean](FieldType.BOOLEAN)

  implicit def jListSchema[T](implicit s: Schema[T]): Schema[java.util.List[T]] =
    ArrayType(s, identity, identity)

  implicit def jArrayListSchema[T](implicit s: Schema[T]): Schema[java.util.ArrayList[T]] =
    ArrayType(s, identity, l => new util.ArrayList[T](l))

  implicit def jMapSchema[K, V](
    implicit ks: Schema[K],
    vs: Schema[V]
  ): Schema[java.util.Map[K, V]] =
    MapType(ks, vs, identity, identity)

  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: RawRecord[T] =
    RawRecord[T](new JavaBeanSchema())

  implicit def javaEnumSchema[T <: java.lang.Enum[T]: ClassTag]: Schema[T] =
    Type[T](FieldType.logicalType(new LogicalType[T, String] {
      private val clazz = scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]]
      private val className = clazz.getCanonicalName
      override def getIdentifier: String = className
      override def getBaseType: FieldType = FieldType.STRING
      override def toBaseType(input: T): String = input.name()
      override def toInputType(base: String): T =
        java.lang.Enum.valueOf[T](clazz, base)
      override def toString(): String =
        s"EnumLogicalType($className, String)"
    }))
}

sealed trait Schema[T] {
  type Repr
  type Decode = Repr => T
  type Encode = T => Repr
}

final case class Record[T] private (
  schemas: Array[(String, Schema[Any])],
  construct: Seq[Any] => T,
  destruct: T => Array[Any]
) extends Schema[T] {
  type Repr = Row

  override def toString: String =
    s"Record(${schemas.toList}, $construct, $destruct)"
}

object Record {
  @inline final def apply[T](implicit r: Record[T]): Record[T] = r
}

final case class RawRecord[T](
  schema: BSchema,
  fromRow: SerializableFunction[Row, T],
  toRow: SerializableFunction[T, Row]
) extends Schema[T] {
  type Repr = Row
}

object RawRecord {
  final def apply[T: ClassTag](provider: SchemaProvider): RawRecord[T] = {
    val td = TypeDescriptor.of(ScioUtil.classOf[T])
    val schema = provider.schemaFor(td)
    def toRow = provider.toRowFunction(td)
    def fromRow = provider.fromRowFunction(td)
    RawRecord(schema, fromRow, toRow)
  }
}

final case class Type[T](fieldType: FieldType) extends Schema[T] {
  type Repr = T
}

final case class OptionType[T](schema: Schema[T]) extends Schema[Option[T]] {
  type Repr = schema.Repr
}

final case class Fallback[F[_], T](coder: F[T]) extends Schema[T] {
  type Repr = Array[Byte]
}

final case class ArrayType[F[_], T](
  schema: Schema[T],
  toList: F[T] => jList[T],
  fromList: jList[T] => F[T]
) extends Schema[F[T]] { // TODO: polymorphism ?
  type Repr = jList[schema.Repr]
  type _T = T
  type _F[A] = F[A]
}

final case class MapType[F[_, _], K, V](
  keySchema: Schema[K],
  valueSchema: Schema[V],
  toMap: F[K, V] => jMap[K, V],
  fromMap: jMap[K, V] => F[K, V]
) extends Schema[F[K, V]] {
  type Repr = jMap[keySchema.Repr, valueSchema.Repr]

  type _K = K
  type _V = V
  type _F[XK, XV] = F[XK, XV]
}

private[scio] case class ScalarWrapper[T](value: T) extends AnyVal

private[scio] object SchemaTypes {
  private[this] def compareRows(s1: BSchema.FieldType, s2: BSchema.FieldType): Boolean = {
    val s1Types = s1.getRowSchema.getFields.asScala.map(_.getType)
    val s2Types = s2.getRowSchema.getFields.asScala.map(_.getType)
    (s1Types.length == s2Types.length) &&
    s1Types.zip(s2Types).forall { case (l, r) => equal(l, r) }
  }

  def equal(s1: BSchema.FieldType, s2: BSchema.FieldType): Boolean =
    (s1.getTypeName == s2.getTypeName) && (s1.getTypeName match {
      case BSchema.TypeName.ROW =>
        compareRows(s1, s2)
      case BSchema.TypeName.ARRAY =>
        equal(s1.getCollectionElementType, s2.getCollectionElementType)
      case BSchema.TypeName.MAP =>
        equal(s1.getMapKeyType, s2.getMapKeyType) && equal(s1.getMapValueType, s2.getMapValueType)
      case _ if s1.getNullable == s2.getNullable => true
      case _                                     => false
    })
}

private object Derived extends Serializable {
  import magnolia._
  def combineSchema[T](ps: Seq[Param[Schema, T]], rawConstruct: Seq[Any] => T): Record[T] = {
    @inline def destruct(v: T): Array[Any] = {
      val arr = new Array[Any](ps.length)
      var i = 0
      while (i < ps.length) {
        val p = ps(i)
        arr.update(i, p.dereference(v))
        i = i + 1
      }
      arr
    }
    val schemas = ps.iterator.map { p =>
      p.label -> p.typeclass.asInstanceOf[Schema[Any]]
    }.toArray

    Record(schemas, rawConstruct, destruct)
  }
}

trait LowPrioritySchemaDerivation {
  import magnolia._

  import language.experimental.macros

  type Typeclass[T] = Schema[T]

  def combine[T](ctx: CaseClass[Schema, T]): Record[T] = {
    val ps = ctx.parameters
    Derived.combineSchema(ps, ctx.rawConstruct)
  }

  import com.spotify.scio.MagnoliaMacros
  implicit def gen[T]: Schema[T] = macro MagnoliaMacros.genWithoutAnnotations[T]
}

private[scio] trait SchemaMacroHelpers {
  import scala.reflect.macros._

  val ctx: blackbox.Context
  import ctx.universe._

  val cacheImplicitSchemas = MacroSettings.cacheImplicitSchemas(ctx)

  def untyped[A: ctx.WeakTypeTag](expr: ctx.Expr[Schema[A]]): ctx.Expr[Schema[A]] =
    ctx.Expr[Schema[A]](ctx.untypecheck(expr.tree.duplicate))

  def inferImplicitSchema[A: ctx.WeakTypeTag]: ctx.Expr[Schema[A]] =
    inferImplicitSchema(weakTypeOf[A]).asInstanceOf[ctx.Expr[Schema[A]]]

  def inferImplicitSchema(t: ctx.Type): ctx.Expr[Schema[_]] = {
    val tpe =
      cacheImplicitSchemas match {
        case FeatureFlag.Enable =>
          tq"_root_.shapeless.Cached[_root_.com.spotify.scio.schemas.Schema[$t]]"
        case _ =>
          tq"_root_.com.spotify.scio.schemas.Schema[$t]"
      }

    val tp = ctx.typecheck(tpe, ctx.TYPEmode).tpe
    val typedTree = ctx.inferImplicitValue(tp, silent = false)
    val untypedTree = ctx.untypecheck(typedTree.duplicate)

    cacheImplicitSchemas match {
      case FeatureFlag.Enable =>
        ctx.Expr[Schema[_]](q"$untypedTree.value")
      case _ =>
        ctx.Expr[Schema[_]](untypedTree)
    }
  }

  implicit def liftTupleTag[A: ctx.WeakTypeTag]: Liftable[TupleTag[A]] = Liftable[TupleTag[A]] {
    x =>
      q"new _root_.org.apache.beam.sdk.values.TupleTag[${weakTypeOf[A]}](${x.getId()})"
  }
}
