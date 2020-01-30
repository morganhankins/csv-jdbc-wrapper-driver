package org.freebase.csv

import java.sql.Connection
import java.sql.SQLException
import java.sql.DriverManager


object CallWrapper {

    fun runQuery(conn: Connection, sql: String) {

        try {
            val stmt = conn.createStatement()
            val rs = stmt.executeQuery(sql)

            println("result $rs")
            // loop through the result set
            while (rs.next()) {
                println("result "+rs.getInt("id") + "\t" + rs.getString("name"))
            }

        } catch (e: SQLException) {
            print(e.message)
        }
    }

    fun wrappedConnection(fileName: String) : Connection {

//        val url = "jdbc:sqlite:$fileName"
        val url = "jdbc:csv:$fileName"
        return DriverManager.getConnection(url)

//        try {
//            DriverManager.getConnection(url).use { conn ->
//                if (conn != null) {
//                    val meta = conn.metaData
//                    println("The driver name is " + meta.driverName)
//                    println("A new database has been created.")
//
//                     val sql = "SELECT id, name from the_table";
//
//                    runQuery(conn,sql );
//                }
//
//            }
//        } catch (e: SQLException) {
//            println(e.message)
//        }

    }
    @JvmStatic
    fun main(args: Array<String>) {

        CsvSqliteDriver()

        val url = "jdbc:csv:/tmp/test.csv"
        val conn = DriverManager.getConnection(url)

        runQuery(conn, "select * from csv_data");

//        val csvLoader = CsvJdbcLoader(conn)
//        csvLoader.loadCSV("/tmp/test.csv", "the_table")

    }
}