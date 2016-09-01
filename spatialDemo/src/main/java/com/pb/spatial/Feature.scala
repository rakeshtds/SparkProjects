package com.pb.spatial

/**
  */
trait Feature extends Serializable {
  def toRowCols(cellSize: Double): Seq[(RowCol, Feature)]
}
