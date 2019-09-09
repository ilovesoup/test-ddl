package com.pingcap.spark;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.CHExtensions;
import org.apache.spark.sql.SparkSession;

public class App {
  private static final Logger logger = LogManager.getLogger(App.class);

  public static void main(String[] args) throws Exception {
    CHExtensions ext = new CHExtensions();
    SparkSession spark = SparkSession
            .builder()
            .withExtensions(ext)
            .config("spark.flash.addresses", "127.0.0.1:9000")
            .config("spark.tispark.pd.addresses", "127.0.0.1:2379")
            .appName("CHSpark Application")
            .master("local")
            .getOrCreate();

    spark.sql("use test");
    spark.sql("show tables").show();

    Type [] types = {
            new IntegralType(IntegralType.IntType.Int, true, true),
            new Varchar(10, true),
            new IntegralType(IntegralType.IntType.TinyInt, false, false),
            new Varchar(1, false),
    };
    String connStr = "jdbc:mysql://127.0.0.1:4000/test?rewriteBatchedStatements=true";
    Table table = new Table(connStr, "root", "", "testddl", Lists.newArrayList(types));
    Table.ActionFactory f = new Table.ActionFactory(table, spark, ext);

    Table.Action actions[] = {
            f.dropColumn(1),
            f.dropColumn(2),
            f.addColumn(-1, new IntegralType(IntegralType.IntType.Int, true, false, "666")).setColumnName("_c1"),
            f.runCheck(() -> {
              long val = spark.sql("select count(*) from testddl where _c1 != 666").collectAsList().get(0).getLong(0);
              if (val != 67) {
                throw new Exception("Wrong result for default value 666");
              }
            }),
            f.addColumn(-1, new IntegralType(IntegralType.IntType.TinyInt, false, true)).setColumnName("_cc"),
            f.renameColumn("_c1", "_c2"),
            f.renameColumn("_cc", "_c1"),
            f.dropColumn("_c1"),
            f.renameColumn(0),
            f.renameColumn(1),
            f.addNDropColumn(3),
            f.modifyColumn(0),
            f.modifyColumn(1),
            f.addColumn(1, new Varchar(10, true)),
            f.modifyColumn(1),
            f.modifyColumn(1),
            f.addColumn(1, new Varchar(10, false)),
            f.modifyColumn(2),
            f.addNotNull(0),
            f.addNotNull(1),
            f.addNotNull(2),
            f.addNotNull(3),
            f.addNotNull(4),
            f.addNotNull(5),
            f.removeNotNull(0),
            f.removeNotNull(1),
            f.removeNotNull(2),
            f.removeNotNull(3),
            f.removeNotNull(4),
            f.removeNotNull(5),
    };

    try {
      for (Table.Action a : actions) {
        a.takeAction();
      }
    } catch (Exception se) {
      logger.error("Error ", se);
    }
  }
}
