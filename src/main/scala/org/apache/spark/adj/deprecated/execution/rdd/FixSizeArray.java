package org.apache.spark.adj.deprecated.execution.rdd;

final public class FixSizeArray {

    public int v0;
    public int v1;
    public int v2;
    public int v3;
    public int v4;
    public int v5;
    private int v6;
    private int size;


    public FixSizeArray(int size) {
        this.size = size;
    }


    public int get(final int i) {
        switch (i) {
            case 0:
                return v0;
            case 1:
                return v1;
            case 2:
                return v2;
            case 3:
                return v3;
            case 4:
                return v4;
            default:
                return 0;
        }
    }


    public void update(final int i, final int v) {
        switch (i) {
            case 0:
                v0 = v; break;
            case 1:
                v1 = v; break;
            case 2:
                v2 = v; break;
            case 3:
                v3 = v; break;
            case 4:
                v4 = v; break;
        }
    }
}
