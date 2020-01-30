package org.freebase.csv

import java.sql.*
import java.util.*
import java.util.logging.Logger

class CsvSqliteDriver @Throws(SQLException::class)
constructor() : Driver {

    private var wrappedDriver: Driver? = null

    @Throws(SQLFeatureNotSupportedException::class)
    override fun getParentLogger(): Logger {
        return wrappedDriver!!.parentLogger
    }

    init {
        try {
            // TODO: For more flexibility, we COULD defer this, and encode this
            //    info into the driver scheme. That way, we could allow users to
            //    dynamically specify the underlying driver and scheme in the JDBC URL.
            wrappedDriver = Class.forName(WRAPPED_DRIVER).newInstance() as Driver
        } catch (e: InstantiationException) {
            e.printStackTrace()
        } catch (e: IllegalAccessException) {
            e.printStackTrace()
        } catch (e: ClassNotFoundException) {
            e.printStackTrace()
        }

    }

    // TODO: is this called?! seems not?
    @Throws(SQLException::class)
    override fun acceptsURL(url: String): Boolean {
        println("yadda:L $url")

        if (url.startsWith(THIS_DRIVER_SCHEME)) {

            return true
        }
        // Remove our special stuff from the URL
        val fixedUrl = fixupUrl(url)
        // If the fixed URL is the same as the original URL, then it’s NOT one of
        // our URLs and we shouldn’t handle it.
        return if (fixedUrl == url) {
            false
        } else wrappedDriver!!.acceptsURL(fixedUrl)

        // Pass the corrected URL to the underlying driver-
        // if the underlying driver can accept the URL, then we can too!
    }

    @Throws(SQLException::class)
    override fun connect(url: String, info: Properties): Connection {
        var url = url

        // Remove our special stuff from the URL
        url = fixupUrl(url)

        try {
            val sqliteUrl = "$url.sqlite"
            val sqliteConn = wrappedDriver!!.connect(sqliteUrl, info)
            val csvLoader = CsvJdbcLoader(sqliteConn)

            val parts = url.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val csvPath = parts[parts.size - 1]

            csvLoader.loadCSV(csvPath, "csv_data")

            return sqliteConn
        } catch (exn: Exception) {
            throw RuntimeException(exn)
        }


        //         And pass through
        //        Connection conn = wrappedDriver.connect(url, info);
        //        return new ConnectionWrapper(conn);
    }

    @Throws(SQLException::class)
    override fun getPropertyInfo(url: String, info: Properties): Array<DriverPropertyInfo> {

        return wrappedDriver!!.getPropertyInfo(url, info)
    }

    override fun getMajorVersion(): Int {
        return wrappedDriver!!.majorVersion
    }

    override fun getMinorVersion(): Int {
        return wrappedDriver!!.minorVersion
    }

    override fun jdbcCompliant(): Boolean {
        return wrappedDriver!!.jdbcCompliant()
    }

    private fun fixupUrl(url: String): String {
        var furl = url
        if (furl.startsWith(THIS_DRIVER_SCHEME)) {
            furl = WRAPPED_DRIVER_SCHEME + furl.substring(THIS_DRIVER_SCHEME.length)
        }


        return furl
    }

    companion object {

        // The class of the driver that we’re wrapping
        //TODO: CHANGE THIS
        val WRAPPED_DRIVER = "org.sqlite.JDBC"

        // The scheme of the driver we’re wrapping. For instance, if the JDBC URL for
        // the DB we want to connect to is:
        //        jdbc:postgresql://db_server.redfintest.com/production
        // then this string should be “jdbc:postgresql:”
        //TODO: CHANGE THIS
        val WRAPPED_DRIVER_SCHEME = "jdbc:sqlite:"

        // The scheme of THIS driver (the wrapper.) Clients use this scheme when they
        // want to use this driver.
        val THIS_DRIVER_SCHEME = "jdbc:csv:"

        init {
            println("DriverWrapper: static init")
            try {
                DriverManager.registerDriver(CsvSqliteDriver())
            } catch (e: Exception) {
            }

        }
    }
}

