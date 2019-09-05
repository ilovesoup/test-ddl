package com.pingcap.spark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Table {
    private static final Logger logger = LogManager.getLogger(Table.class);
    private final Connection conn;
    private int autoNum = 0;
    private String tableName;
    private ColunmNamer colNamer = new ColunmNamer();
    private List<Column> schema;
    private static final int INSERT_NUM = 100;

    public String getTableName() {
        return tableName;
    }

    public int getColumnNumber() {
        return schema.size();
    }

    class Column {
        public final String name;
        public final Type t;

        public Column(String name, Type t) {
            this.name = name;
            this.t = t;
        }

        public Column(Type t) {
            this.name = colNamer.getName();
            this.t = t;
        }

        @Override
        public String toString() {
            return String.format("`%s` %s", name, t.ddlString());
        }
    }

    static class ColunmNamer {
        private int inc = 0;
        String getName() {
            return String.format("c%d", inc++);
        }
    }

    static class Action {
        enum Op {
            AddColumn, DropColumn, ModifyType, RenameColumn, AddAndDrop
        }
        private final Table table;
        private final Op op;
        private final int pos;
        private final Type t;
        private final SparkSession spark;

        private Action(Table table, Op op, int pos, Type t, SparkSession spark) {
            this.table = table;
            this.op = op;
            this.pos = pos;
            this.t = t;
            this.spark = spark;
        }

        public void takeAction() throws Exception {
            switch(op) {
                case AddColumn:
                    table.addColumn(pos, t);
                    break;
                case DropColumn:
                    table.dropColumn(pos);
                    break;
                case ModifyType:
                    int curPos = pos;
                    while (!table.enlargeColumn(curPos) && curPos < table.getColumnNumber()) {
                        curPos++;
                    }
                    break;
                case RenameColumn:
                    table.renameColumn(pos);
                    break;
                case AddAndDrop:
                    table.addNDropColumn(pos);
                    break;
                default:
                    throw new Exception("Wrong Action Type");
            }
            table.insert(INSERT_NUM);
            if (spark != null) {
                logger.info("SPARK START");
                spark.sql("select * from " + "test."+ table.getTableName()).show();
                logger.info("CHECK PASSED");
            }
        }
    }

    static public class ActionFactory {
        private final Table table;
        private final SparkSession spark;
        public ActionFactory(Table table, SparkSession spark) {
            this.table = table;
            this.spark = spark;
        }

        public Action addColumn(int pos, Type t) {
            return new Action(table, Action.Op.AddColumn, pos, t, spark);
        }

        public Action dropColumn(int pos) {
            return new Action(table, Action.Op.DropColumn, pos, null, spark);
        }

        public Action modifyColumn(int pos) {
            return new Action(table, Action.Op.ModifyType, pos, null, spark);
        }

        public Action renameColumn(int pos) {
            return new Action(table, Action.Op.RenameColumn, pos, null, spark);
        }

        public Action addNDropColumn(int pos) {
            return new Action(table, Action.Op.AddAndDrop, pos, null, spark);
        }
    }

    public Table(String connStr, String user, String password, String tableName, ArrayList<Type> schema) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection(connStr, user, password);
        createTable(tableName, schema);
    }

    private void createTable(String tableName, ArrayList<Type> types) throws Exception {
        this.tableName = tableName;

        List<Column> schema = new ArrayList<>(types.size());
        for (Type t : types) {
            schema.add(new Column(t));
        }
        this.schema = schema;

        List<String> cols = new ArrayList<>();
        for (int i = 0; i < schema.size(); i ++) {
            cols.add(schema.get(i).toString());
        }

        manipulateTable(String.format("drop table if exists %s", tableName));
        String sql = String.format("create table %s (pk bigint, %s)", tableName, String.join(", ", cols));
        manipulateTable(sql);
        insert(INSERT_NUM);
    }

    public void insert(int num) throws Exception {
        Statement stmt = conn.createStatement();

        ArrayList cols[] = {new ArrayList<>(), new ArrayList<>(), new ArrayList<>()};

        for (Column c : schema) {
            Type t = c.t;
            cols[0].add(t.getMin());
            cols[1].add(t.getMax());
            if (t.isNotNull()) {
                cols[2].add(t.getMin());
            } else {
                cols[2].add(t.getNull());
            }
        }
        String values[] = {
                String.join(",", cols[0]),
                String.join(",", cols[1]),
                String.join(",", cols[2]),
        };

        for (int i = 1; i < num; i++) {
            stmt.addBatch(String.format("insert into %s values (%d, %s);", tableName, autoNum++, values[i % 3]));
        }
        stmt.executeBatch();
        stmt.close();
        logger.info("Inserted {} rows", num);
    }

    private Column addColumn(int pos, Type t) throws Exception {
        Column c = new Column(t);
        addColumn(pos, c);
        return c;
    }

    private void addColumn(int pos, Column c) throws Exception {
        if (pos > schema.size()) {
            pos = schema.size();
        }
        String posStr;
        if (pos == -1) {
            posStr = "";
            pos = schema.size();
        } else if (pos == 0) {
            posStr = "first";
        } else {
            posStr = String.format("after %s", schema.get(pos - 1).name);
        }
        schema.add(pos, c);
        String sql = String.format("alter table %s add column %s %s", tableName, c.toString(), posStr);
        manipulateTable(sql);
    }

    private Column dropColumn(int pos) throws Exception {
        if (pos > schema.size()) {
            pos = schema.size();
        }
        Column c = schema.remove(pos);
        String sql = String.format("alter table %s drop column %s", tableName, c.name);
        manipulateTable(sql);
        return c;
    }

    private Column renameColumn(int pos) throws Exception {
        if (pos > schema.size()) {
            pos = schema.size();
        }
        Column newCol = new Column(schema.get(pos).t);
        Column oldCol = schema.set(pos, newCol);
        String sql = String.format("alter table %s change column %s %s",
                tableName, oldCol.name, newCol.toString());
        manipulateTable(sql);
        return newCol;
    }

    private void addNDropColumn(int pos) throws Exception {
        if (pos > schema.size()) {
            pos = schema.size();
        }
        Column oldCol = dropColumn(pos);
        addColumn(pos, oldCol);

    }

    private boolean enlargeColumn(int pos) throws Exception {
        if (pos > schema.size()) {
            pos = schema.size();
        }
        Column c = schema.get(pos);
        Type t = c.t.enlarge();
        if (t == null) {
            return false;
        }
        schema.set(pos, new Column(c.name, c.t.enlarge()));
        c = schema.get(pos);
        String sql = String.format("alter table %s modify column %s", tableName, c.toString());
        manipulateTable(sql);
        return true;
    }

    private void manipulateTable(String sql) throws Exception {
        logger.info("DDL: " + sql);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
    }
}