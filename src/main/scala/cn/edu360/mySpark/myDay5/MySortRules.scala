package cn.edu360.mySpark.myDay5

object MySortRules {


  implicit object XianRouRule extends Ordering[XianRou]{
    override def compare(x: XianRou, y: XianRou): Int = {
      if (x.fv==y.fv){
        x.age-y.age
      }else{
        -(x.fv-y.fv)
      }
    }
  }
}
