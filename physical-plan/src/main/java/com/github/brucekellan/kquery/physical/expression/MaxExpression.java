package com.github.brucekellan.kquery.physical.expression;

public class MaxExpression implements AggregateExpression {

    private Expression expression;

    public MaxExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public Expression inputExpression() {
        return expression;
    }

    @Override
    public Accumulator createAccumulator() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("MAX(%s)", expression);
    }

    public static class MaxAccumulator implements Accumulator {
        private Object value = null;

        public MaxAccumulator(Object value) {
            this.value = value;
        }

        @Override
        public void accumulate(Object value) {
            if (value != null) {
                if (this.value == null) {
                    this.value = value;
                }
                Object isMax = null;
                if (value instanceof Byte) {
                    if ((Byte) value > (Byte) this.value) {
                        this.value = value;
                    }
                } else if (value instanceof Short) {
                    if ((Short) value > (Short) this.value) {
                        this.value = value;
                    }
                } else if (value instanceof Integer) {
                    if ((Integer) value > (Integer) this.value) {
                        this.value = value;
                    }
                } else if (value instanceof Long) {
                    if ((Long) value > (Long) this.value) {
                        this.value = value;
                    }
                } else if (value instanceof Float) {
                    if ((Float) value > (Float) this.value) {
                        this.value = value;
                    }
                } else if (value instanceof Double) {
                    if ((Double) value > (Double) this.value) {
                        this.value = value;
                    }
                } else if (value instanceof String) {
                    if (((String) value).compareTo((String) this.value) > 0) {
                        this.value = value;
                    }
                } else {
                    throw new UnsupportedOperationException(
                            "MAX is not implemented for data type: " + value.getClass().getName());
                }
            }
        }

        @Override
        public Object finalValue() {
            return value;
        }
    }
}
