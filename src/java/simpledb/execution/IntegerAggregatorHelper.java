package simpledb.execution;

public class IntegerAggregatorHelper {
    public static Integer aggregate(Integer prevValue, Aggregator.Op op, Integer newValue, Integer count) {
        switch (op) {
            case MIN:
                if (prevValue == null || newValue < prevValue) {
                    return newValue;
                } else {
                    return prevValue;
                }
            case MAX:
                if (prevValue == null || newValue > prevValue) {
                    return newValue;
                } else {
                    return prevValue;
                }
            case COUNT:
                if (prevValue == null)
                    return 1;
                return prevValue + 1;
            case AVG:
//                if (prevValue == null)
//                    return newValue;
//                return (count * prevValue + newValue) / (count + 1);
            case SUM:
                if (prevValue == null)
                    return newValue;
                return prevValue + newValue;
            default:
                throw new UnsupportedOperationException("not implemented");
        }
    }
}
