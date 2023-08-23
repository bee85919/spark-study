import org.apache.spark.util.AccumulatorV2

class SetAccumulator[T](var _value: scala.collection.mutable.Set[T]) extends AccumulatorV2[T, scala.collection.immutable.Set[T]] {
  def this() = this(scala.collection.mutable.Set.empty[T])
  override def isZero(): Boolean = _value.isEmpty
  override def copy(): AccumulatorV2[T, scala.collection.immutable.Set[T]] = new SetAccumulator[T](_value)
  override def reset(): Unit = _value = scala.collection.mutable.Set.empty[T]
  override def add(v: T): Unit = _value += v
  override def merge(other: AccumulatorV2[T, scala.collection.immutable.Set[T]]): Unit = _value ++= other.value
  override def value(): scala.collection.immutable.Set[T] =_value.to[collection.immutable.Set]
}

val setAccum = new SetAccumulator[String]()
spark.sparkContext.register(setAccum, "My Set Accum") // Optional, name it for SparkUI

spark.sparkContext.parallelize(Seq("a", "b", "a", "b", "c")).foreach(s => setAccum.add(s))

setAccum.value