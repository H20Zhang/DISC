package org.apache.spark.Logo.Plan;



import org.apache.spark.Logo.UnderLying.dataStructure.PatternInstance;
import scala.Function1;
        import scala.reflect.ScalaSignature;




public class FilteringCondition2
{


    private Function1<PatternInstance, Boolean> f;
    private boolean isStrictCondition;

    public FilteringCondition2(Function1<PatternInstance, Boolean> f, boolean isStrictCondition) {}

    public boolean isStrictCondition()
    {

        return this.isStrictCondition;
    }

    public Function1<PatternInstance, Boolean> f()
    {
        return this.f;
    }


}