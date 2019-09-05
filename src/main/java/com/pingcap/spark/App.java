package com.pingcap.spark;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class App {
  private static final Logger logger = LogManager.getLogger(App.class);

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
            .builder()
            .config("spark.sql.extensions", "org.apache.spark.sql.CHExtensions")
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
    Table.ActionFactory f = new Table.ActionFactory(table, spark);
    Table.Action actions[] = {
            f.dropColumn(1),
            f.dropColumn(2),
            f.addColumn(-1, new IntegralType(IntegralType.IntType.Int, true, false)),
            f.addColumn(-1, new IntegralType(IntegralType.IntType.TinyInt, false, true)),
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
