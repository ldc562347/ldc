package util

trait Tag {
  def mkTags(args:Any*):List[(String,Int)]
}
