package org.freebase.csv

import java.io.File
import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.Date

import org.freebase.csv.reader.CsvReader
import org.freebase.csv.reader.CsvParser

import org.freebase.csv.reader.*

interface SqliteType {
    fun test(v : Any) : Boolean
    val sqliteType : String
    fun setValueOnStmt(ps: PreparedStatement, col : Int, v : Any)
}

class TextSqliteType : SqliteType {

    override fun test(v : Any) : Boolean {
        return true
    }
    override val sqliteType : String = "text"

    override fun setValueOnStmt(ps: PreparedStatement, col : Int, v : Any) {
        ps.setString(col, v.toString())
    }
}
// determine field types, make [test, create_type, extract_fun] for each type
// get sample, count the tests, use that type

class SqliteTypeGuesser {

    fun guessTypes(csvPath: String): Map<String, SqliteType> {
        val csvParser: CsvParser = CsvReader().parse(File(csvPath), StandardCharsets.UTF_8)

        val headerRow = csvParser.nextRow()

        val data = mutableListOf<CsvRow>()

        var nextLine = csvParser.nextRow()

        while (nextLine != null) {
            data.add(nextLine)
            nextLine = csvParser.nextRow()
        }

        val r = mutableMapOf<String, SqliteType>()
        for (i in 0 until data.size-1) {

            r[headerRow.getField(i).trim()] = guessType(headerRow, data, i)
        }
        return r
    }

    fun guessType(headerRow: CsvRow, data: List<CsvRow>, col: Int): SqliteType {
        println("guessType: ${headerRow.getField(col).trim()}")
        return TextSqliteType()
    }
}


// much inspired by viralpatel.net
class CsvJdbcLoader(private val connection: Connection?) {

    var seprator: Char = ' '

    init {
        //Set default separator
        this.seprator = ','
    }

    fun createTable(csvPath : String, tableName: String) : Map<String, SqliteType>{

        val colTypes = SqliteTypeGuesser().guessTypes(csvPath)

        val m = colTypes.map { (k,v) -> "${k.trim()} ${v.sqliteType}" }.joinToString(", ")

        val sql = "CREATE TABLE $tableName ( $m );"
        println("create sql: $sql")

        val ps1 = this.connection!!.prepareStatement("DROP TABLE IF EXISTS $tableName")
        ps1.execute()

        val ps = this.connection!!.prepareStatement(sql)
        ps.execute()

        return colTypes
    }

    @Throws(Exception::class)
    fun loadCSV(csvFile: String, tableName: String) {

        this.connection ?: throw Exception("Not a valid connection.")

        val csvParser: CsvParser = CsvReader().parse(File(csvFile), StandardCharsets.UTF_8)

        val headerRow = csvParser.nextRow() ?: throw FileNotFoundException(
            "No columns defined in given CSV file." + "Please check the CSV file format."
        )

        val colTypeMap = createTable(csvFile, tableName)

        var questionmarks = "?,".repeat(headerRow.fieldCount)
        questionmarks = questionmarks.subSequence(0, questionmarks.length - 1) as String

        var query = SQL_INSERT.replaceFirst(TABLE_REGEX.toRegex(), tableName)
        query = query
            .replaceFirst(KEYS_REGEX.toRegex(), headerRow.fields.joinToString(","))
        query = query.replaceFirst(VALUES_REGEX.toRegex(), questionmarks)


        println("Query: $query")

        var con: Connection? = null
        var ps: PreparedStatement? = null
        try {
            con = this.connection
            con.autoCommit = false
            ps = con.prepareStatement(query)


            val batchSize = 1000
            var count = 0
            var date: Date? = null

            var nextLine = csvParser.nextRow()

            while (nextLine != null) {
                var index = 0
                for (string in nextLine.fields) {

                val fieldName = headerRow.getField(index).trim()
                    colTypeMap[fieldName]?.setValueOnStmt(ps, (index++)+1, string)
                }
                ps!!.addBatch()

                if (++count % batchSize == 0) {
                    ps.executeBatch()
                }
                nextLine = csvParser.nextRow()
            }
            ps!!.executeBatch() // insert remaining records
            con.commit()
        } catch (e: Exception) {
            con!!.rollback()
            e.printStackTrace()
            throw Exception(
                "Error occured while loading data from file to database." + e.message
            )
        } finally {
            ps?.close()
//            con?.close()
            csvParser.close()
        }
    }

    companion object {
        private val SQL_INSERT = "INSERT INTO \${table}(\${keys}) VALUES(\${values})"
        private val TABLE_REGEX = "\\$\\{table\\}"
        private val KEYS_REGEX = "\\$\\{keys\\}"
        private val VALUES_REGEX = "\\$\\{values\\}"
    }

}