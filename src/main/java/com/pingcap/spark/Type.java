package com.pingcap.spark;

public abstract class Type {
    protected boolean isNotNull;
    abstract Type enlarge();
    abstract String ddlString();
    abstract String getMin();
    abstract String getMax();
    String getNull() { return "NULL"; }
    boolean isNotNull() { return isNotNull; }
}

class IntegralType extends Type {
    public enum IntType {
        TinyInt, SmallInt, Int, MediumInt, BigInt
    }

    private IntType detailType;
    private boolean isUnsigned;

    boolean isUnsigned() {
        return isUnsigned;
    }

    private static String [][] minVal =  {
            {"-128", "-32768", "-8388608", "-2147483648", String.format("%d", Long.MIN_VALUE)},
            {"0", "0", "0", "0", "0"}
    };

    private static String [][] maxVal = {
            {"127", "32767", "8388607", "2147483647", "18446744073709551615" },
            {"255", "65535", "16777215", "4294967295", String.format("%d", Long.MAX_VALUE)}
    };

    public IntegralType(IntType detailType, boolean unsigned, boolean notNull) {
        this.detailType = detailType;
        this.isUnsigned = unsigned;
        this.isNotNull = notNull;
    }

    @Override
    Type enlarge() {
        IntType newType = IntType.values()[detailType.ordinal()];
        return new IntegralType(newType, isUnsigned(), isNotNull());
    }

    @Override
    String ddlString() {
        return String.format("%s %s %s",
                detailType.name(),
                isUnsigned() ? "UNSIGNED" : "",
                isNotNull() ? "NOT NULL" : "");
    }

    @Override
    String getMin() {
        return minVal[isUnsigned() ? 1 : 0][detailType.ordinal()];
    }

    @Override
    String getMax() {
        return maxVal[isUnsigned() ? 1 : 0][detailType.ordinal()];
    }
}

class Varchar extends Type {
    private int length;
    public Varchar(int length, boolean notNull) {
        this.isNotNull = notNull;
        this.length = length;
    }

    @Override
    public Type enlarge() {
        return new Varchar(length + 1, isNotNull());
    }

    @Override
    public String ddlString() {
        return String.format("VARCHAR(%d) %s", length, isNotNull() ? "NOT NULL" : "");
    }

    @Override
    public String getMin() {
        return "''";
    }

    @Override
    public String getMax() {
        return "'" + new String(new char[length]).replace('\0', 'A') + "'";
    }
}
