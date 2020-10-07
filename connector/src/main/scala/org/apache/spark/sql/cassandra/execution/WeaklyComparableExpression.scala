package org.apache.spark.sql.cassandra.execution

import org.apache.commons.lang3.builder.HashCodeBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

/** Implements less strict [[Expression]] equals than the one implemented by
  * expression subclasses. It's used to match [[NamedExpression]] by name and
  * type, it disregards other [[NamedExpression]] attributes like `qualifier`.
  * Thanks to this SCC is able to match join attributes with source
  * attributes. See SPARKC-613 for details.
  */
class WeaklyComparableExpression(val expression: Expression) extends Serializable {

  override def equals(obj: Any): Boolean = {
    if (obj == null)
      return false
    obj match {
      case ce: WeaklyComparableExpression =>
        (this.expression, ce.expression) match {
          case (first: NamedExpression, second: NamedExpression) =>
            first.name == second.name && first.dataType == second.dataType
          case (first, second) =>
            first == second
        }
      case _ =>
        false
    }
  }

  override val hashCode: Int = {
    expression match {
      case ne: NamedExpression => new HashCodeBuilder().append(ne.name).append(ne.dataType).hashCode()
      case _ => expression.hashCode()
    }
  }
}

object WeaklyComparableExpression {
  implicit def asWeaklyComparable(expression: Expression): WeaklyComparableExpression =
    new WeaklyComparableExpression(expression)
}
