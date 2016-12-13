using System;
using System.Data;
using System.Data.SqlTypes;
using System.Collections;
using System.Text;
using System.Collections.Generic;
using System.ServiceProcess;
using Microsoft.Win32;
using System.Threading;
using System.Security;
using MGL.Data.DataUtilities;
using DataNirvana.DomainModel.Database;
//using MGL.DomainModel.Database;


//-------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {

    //----------------------------------------------------------------------------------------------------------------------------
    ///<summary>
    ///
    ///       Name:       	DatabaseInformation
    ///       Description:	Extracts Information from the database using ADO.NET
    ///       Type:				Extraction
    ///       Author:			Josu Ramirez & Edgar Scrase
    ///       Date:				September 2004
    ///       Version:			1.0
    ///
    ///         Notes:           As this class is so commonly used and is so low level, this class does not write errors directly to the
    ///         MGLErrorLog.  Rather, it writes the errors to an internal list, that can then be accessed with G(g)etErrors.
    ///
    ///</summary>
    public class DatabaseWrapper : DatabaseWrapperBase {

        // Rather oldschool way of getting the class name for the Logger .....
        private string thisClassName = "DataNirvana.Database.DatabaseWrapper";

        //-------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Stores the DB connection information
        /// </summary>
        private DatabaseConnectionInfo dbConInfo = new DatabaseConnectionInfo();


        //-------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The DBWrapper object which does all the grunt work of the connections to the SQL databases ...
        /// </summary>
//        private DatabaseWrapperBase oDB = null;


        //-------------------------------------------------------------------------------------------------------
        /// <summary>With the entire connection string as the argument</summary>
        public DatabaseWrapper(string dbProvider, SecureString connectionString) {

            DoConstructorStuff(new DatabaseConnectionInfo("", dbProvider, "", new SecureString(), new SecureString(), 0), connectionString);
        }
        //-------------------------------------------------------------------------------------------------------
        /// <summary>With the entire connection string individual arguments</summary>
        public DatabaseWrapper(string sDBProvider, string sServer, string sDatabase, SecureString sUsername, SecureString sPassword, int sPort) {

            DoConstructorStuff(new DatabaseConnectionInfo(sServer, sDBProvider, sDatabase, sUsername, sPassword, sPort), null);
        }

        //-------------------------------------------------------------------------------------------------------
        /// <summary>Logs errors directly to the MGLErrorLog.  G(g)etErrors will still return the (same) errors if UseMGLErrorLog is set to true</summary>
        public DatabaseWrapper(bool useStaticMGLErrorLog, string sDBProvider, string sServer, string sDatabase, SecureString sUsername, SecureString sPassword, int sPort) {

            DoConstructorStuff(new DatabaseConnectionInfo(sServer, sDBProvider, sDatabase, sUsername, sPassword, sPort), null);
        }

        //-------------------------------------------------------------------------------------------------------
        /// <summary>
        ///      24-Aug-15 - the SVG database is no more!!!  Use D3 and the mapping apps for visualisation and MapInfo / Arc for crunching
        /// </summary>
        public DatabaseWrapper(ConfigurationInfo lcf) {
            DatabaseConnectionInfo dbConInfo = lcf.DbConInfo;
            DoConstructorStuff(dbConInfo, null);
        }
        //-------------------------------------------------------------------------------------------------------
        /// <summary>
        ///      24-Aug-15 - the SVG database is no more!!!  Use D3 and the mapping apps for visualisation and MapInfo / Arc for crunching
        /// </summary>
        public DatabaseWrapper(DatabaseConnectionInfo dbConInfo) {
            DoConstructorStuff(dbConInfo, null);
        }


        //-------------------------------------------------------------------------------------------------------
        private void DoConstructorStuff(DatabaseConnectionInfo dbConInfo, SecureString connectionString) {

            // store the values in the global variable ...
            this.dbConInfo = dbConInfo;

            // Reset the DBWrapper object ...
            //this.oDB = null;

            if ( connectionString != null && connectionString.Length > 0) {

                // Note that this overload does not call do constructor stuff!
                if (dbConInfo.TYPE != null && dbConInfo.TYPE != "" && connectionString != null && connectionString.Length > 0) {
//                    this.oDB = new DatabaseWrapperBase(.GetADONETWrapper(dbConInfo.TYPE);
                    this.ConnectionString = connectionString;
                }

            } else {
                // 1. Create DBWrapper
                //                this.oDB = DatabaseWrapperBase.GetADONETWrapper(dbConInfo.TYPE);
                // Replaced by method below
                // this.oDB.ConnectionString = "Server=" + m_sServer + ";User ID=" + m_sUsername + ";Password=" + m_sPassword + ";Database=" + m_sDatabase + ";";
                //this.oDB.ConnectionString = GetConnectionString(dbConInfo);
                this.ConnectionString = GetConnectionString(dbConInfo);
            }


        }

        //-------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     See this useful input on connection strings - https://www.connectionstrings.com/mysql/
        /// </summary>
        private SecureString GetConnectionString(DatabaseConnectionInfo dbConInfo) {

            SecureString ss = null;

            // These are instantiated globally in this method so that we can kill them in the finally clause ...
            StringBuilder connectionString = new StringBuilder();
            StringBuilder un = new StringBuilder();
            StringBuilder pw = new StringBuilder();
            StringBuilder certPath = new StringBuilder();
            StringBuilder certPW = new StringBuilder();

            try {

                pw = SecureStringWrapper.Decrypt(dbConInfo.PASSWORD);
                un = SecureStringWrapper.Decrypt(dbConInfo.USER);
                certPath = SecureStringWrapper.Decrypt(dbConInfo.SSLCertificatePath);
                certPW = SecureStringWrapper.Decrypt(dbConInfo.SSLCertificatePassword);

                // These four are mandatory ...
                connectionString.Append("Server=" + dbConInfo.HOST + ";");
                connectionString.Append("Database=" + dbConInfo.NAME + ";");
                connectionString.Append("User ID=" + un + ";");

                // if the password contains " or ; it needs to be quoted appropriately ...
                // note that if the password contains both " and ' then there could be problems!  So we need to escape any existing single quotes
                //  For the record - quotes are shite database password characters!
                if (pw.ToString().Contains(";") == true || pw.ToString().Contains(" ") == true || pw.ToString().Contains("\"") == true) {
                    // needs single quotes - and escape any existing quotes
                    connectionString.Append("Password=" + DataUtilities.Quote(pw.ToString()) + ";");

                } else {
                    // needs no quotes
                    connectionString.Append("Password=" + pw + ";");
//                    connectionString.Append("Password=\"" + pw + "\";");
                }


                // Port is optional and should only be used if not 0
                if (dbConInfo.PORT > 0) {
                    connectionString.Append("Port=" + dbConInfo.PORT + ";");
                }

                // and then the SSL stuff ONLY if the SSL is required - note that there is a "preferred" mode as well, but that
                // is too detailed for now!
                if (dbConInfo.SSLRequired == true) {
                    connectionString.Append("SSL Mode=Required;");
                    connectionString.Append("CertificateFile=" + certPath + ";");

                    //if (certPW.Length == 0) {
                        // 8-Oct-2015 - do nothing to try to make the server work!!!
                        // (this leaving it blank didnt work!!!!)
                    if (certPW.ToString().Contains(";") == true || certPW.ToString().Contains(" ") == true || certPW.ToString().Contains("\"") == true) {
                        // needs single quotes - and escape any existing quotes
                        connectionString.Append("CertificatePassword=" + DataUtilities.Quote(certPW.ToString()) + ";");
                    } else {
                        connectionString.Append("CertificatePassword=" + certPW + ";");
                        //connectionString.Append("CertificatePassword=\"" + certPW + "\";");
                    }
                }

                // This is what the syntax looks like:
                /*
                    + ";"
                    + "CertificateFile=C:/Docs/OpenSSL_DataNirvana/XXXXCertificate.pfx;"
                    +  "CertificatePassword=;"
                    +  "SSL Mode=Required"
                */
                // Now lets encrypt the connection string as a secure string ...
                ss = SecureStringWrapper.Encrypt(connectionString.ToString());

                // DANGEROUS - REMOVE LATER----------------------------------------------------------------------
                //Logger.LogError("MySQL Connection string: "+SecureStringWrapper.Decrypt(ss));


            } catch (Exception ex) {

                Logger.LogError(9, "Problem generating the database connection string: " + ex.ToString());

            } finally {
                // clear all the objects officially so that we dont need to rely on GC
                connectionString = null;
                pw = null;
                un = null;
                certPath = null;
                certPW = null;

            }

            return ss;
        }


        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method
        //--- Overloaded:	Yes
        //--- Return Value:	None
        //--- Purpose:		Executes a SQL statement and returns nothing. (Transactional)
        //---------------------------------------------------------------------------------------------------------------
        public void RunSqlNonQueryTransactional(string sSql) {
            try {
                this.BeginTransaction();
                this.ExecuteNonQuery(sSql);
                this.CommitTransaction();

            } catch (Exception ex) {
                this.CommitTransaction();

                Logger.LogError( thisClassName, "RunSqlNonQueryTransactional", "", ex, sSql, LoggerErrorTypes.Database);
            }
        }

        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method
        //--- Overloaded:	Yes
        //--- Return Value:	None
        //--- Purpose:		Executes a SQL statement and returns nothing. (Non Transactional)
        //---------------------------------------------------------------------------------------------------------------
        public void RunSqlNonQuery(string sSql) {

            this.ExecuteNonQuery(sSql);

        }

        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method
        //--- Overloaded:	Yes
        //--- Return Value:	MySqlDataReader
        //--- Purpose:		Executes a SQL statement.
        //---------------------------------------------------------------------------------------------------------------
        public IDataReader RunSqlReader(string sSql) {
            IDataReader oResult = null;

            //Get results from execution
            oResult = this.ExecuteReader(sSql);

            if (oResult == null) { // error trapped
                Logger.LogError(thisClassName, "RunSqlReader", "", null, sSql, LoggerErrorTypes.Database);
            }
            return oResult;

        }

        //---------------------------------------------------------------------------------------------------------------
        //public bool QuickConnectionCheck() {
        //    return this.QuickConnectionCheck();
        //}

        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method - WARNING - MAY ONLY WORKS FOR MYSQL!!!!!
        //--- Overloaded:	Yes
        //--- Return Value:	string[]
        //--- Purpose:		Returns the table names of the database.
        //---------------------------------------------------------------------------------------------------------------
        public string[] getTableNames() {
            return GetTableNames();
        }
        //---------------------------------------------------------------------------------------------------------------
        public string[] GetTableNames() {
            return GetTableNames("");
        }

        //---------------------------------------------------------------------------------------------------------------
        public string[] GetTableNames(string tablenamesLike) {
            string[] oTables = null;
            IDataReader oMyReader = null;
            try {
                string sql = "SHOW TABLES;";
                //Get results from execution
                if (tablenamesLike != null && tablenamesLike.Length > 0) {
                    sql = "SHOW TABLES LIKE '" + tablenamesLike + "';";
                }

                oMyReader = this.ExecuteReader(sql);

                //--- Get data from MySqlDataReader and return the string[]
                ArrayList oTableNames = new ArrayList();
                while (oMyReader.Read()) {
                    oTableNames.Add(oMyReader.GetString(0));
                }
                oMyReader.Close();

                // convert to an array and return
                oTables = new String[oTableNames.Count];
                for (int i = 0; i < oTableNames.Count; i++) {
                    oTables[i] = (string)oTableNames[i];
                }
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetTableNames", "", ex, tablenamesLike, LoggerErrorTypes.Database);
            } finally {
                if (oMyReader != null && !oMyReader.IsClosed) {
                    oMyReader.Close();
                }
            }

            return oTables;
        }

        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method - WARNING - MAY ONLY WORKS FOR MYSQL!!!!!
        //--- Overloaded:	Yes
        //--- Return Value:	string[]
        //--- Purpose:		Returns the column names of the specific table.
        //---------------------------------------------------------------------------------------------------------------
        public string[] GetColumnNames(string sTableName, List<string> colNamesToExclude) {
            return GetColumnNames(sTableName, colNamesToExclude, String.Empty);
        }
        //---------------------------------------------------------------------------------------------------------------
        public string[] GetColumnNames(string sTableName, List<string> colNamesToExclude, string tableAlias) {
            string[] oColumns = null;
            IDataReader oMyReader = null;
            try {

                //Get results from execution
                oMyReader = this.RunSqlReader("SHOW COLUMNS FROM " + sTableName.ToLower());

                //--- Get data from MySqlDataReader and return the string[]
                List<string> oColumnNames = new List<string>();
                string col = null;
                while (oMyReader.Read()) {
                    col = oMyReader.GetString(0);

                    if (colNamesToExclude != null && colNamesToExclude.Count > 0) {
                        bool isExcluded = false;
                        foreach (string colNameToExclude in colNamesToExclude) {
                            if (col.Equals(colNameToExclude, StringComparison.CurrentCultureIgnoreCase)) {
                                isExcluded = true;
                                break;
                            }
                        }

                        if (!isExcluded) {
                            if (tableAlias != null && tableAlias != String.Empty)
                                col = tableAlias + "." + col;
                            oColumnNames.Add(col);
                        }
                    } else {
                        if (tableAlias != null && tableAlias != String.Empty)
                            col = tableAlias + "." + col;
                        oColumnNames.Add(col);
                    }

                }
                oMyReader.Close();

                // convert to an array and return
                oColumns = new string[oColumnNames.Count];
                for (int i = 0; i < oColumnNames.Count; i++) {
                    oColumns[i] = (string)oColumnNames[i];
                }
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetColumnNames", "", ex, null, LoggerErrorTypes.Database);
                return null;
            } finally {
                if (oMyReader != null && !oMyReader.IsClosed) {
                    oMyReader.Close();
                }
            }
            return oColumns;
        }
        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method - WARNING - MAY ONLY WORKS FOR MYSQL!!!!!
        //--- Overloaded:	Yes
        //--- Return Value:	string[]
        //--- Purpose:		Returns the column names of the specific table. 1, upperCase, 0, lowercase!!!
        //---------------------------------------------------------------------------------------------------------------
        public ArrayList GetColumnNames(string sTableName, int uppercase) {
            ArrayList oColumnNames = null;
            IDataReader oMyReader = null;
            try {
                //Get results from execution
                if (sTableName.Contains(".")) {
                    oMyReader = this.ExecuteReader("SHOW FIELDS FROM " + sTableName);
                } else {
                    oMyReader = this.ExecuteReader("DESCRIBE " + sTableName);
                }

                //--- Get data from MySqlDataReader and return the string[]
                oColumnNames = ArrayList.Synchronized(new ArrayList(10));
                while (oMyReader.Read()) {
                    if (uppercase == 1) {
                        oColumnNames.Add(oMyReader.GetString(0).ToUpper());
                    } else {
                        oColumnNames.Add(oMyReader.GetString(0).ToLower());
                    }
                }
                oMyReader.Close();

            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetColumnNames", "", ex, null, LoggerErrorTypes.Database);
            } finally {
                if (oMyReader != null && !oMyReader.IsClosed) {
                    oMyReader.Close();
                }
            }

            return oColumnNames;
        }
        //---------------------------------------------------------------------------------------------------------------
        public List<string> GetColumnNames(string sTableName) {
            List<string> columnNames = null;
            try {
                //Get results from execution
                string[] data = GetColumnNames(sTableName, null);

                if (data != null && data.Length > 0) {
                    columnNames = new List<string>(data);
                }

            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetColumnNames", sTableName, ex, null, LoggerErrorTypes.Database);
                return null;
            }

            return columnNames;
        }




        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method
        //--- Overloaded:	Yes
        //--- Return Value:	True if the query its all right, false if there is an error
        //--- Purpose:		Returns the column names of the specific table.
        //---------------------------------------------------------------------------------------------------------------
        public bool CheckQuery(string sSql) {

            bool bResult = false;
            IDataReader myReader = null;
            try {
                //Get results from execution
                myReader = this.ExecuteReader(sSql);
                bResult = true;

            } catch (Exception ex) {
                Logger.LogError(thisClassName, "CheckQuery", "", ex, sSql, LoggerErrorTypes.Database);
            } finally {
                if (myReader != null && !myReader.IsClosed) {
                    myReader.Close();
                }
            }

            return bResult;
        }

        //---------------------------------------------------------------------------------------------------------------
        //--- Public Method - WARNING - MAY ONLY WORK FOR MYSQL!!!!!
        //--- Overloaded:	Yes
        //--- Return Value:	true if it deletes the table, false if error
        //--- Purpose:		Deletes the specific table.
        //---------------------------------------------------------------------------------------------------------------
        public bool DeleteTable(string sTableName) {
            bool success = false;

            try {
                this.ExecuteSQL("DROP TABLE " + sTableName + ";");
                success = true;
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "DeleteTable", sTableName, ex, null, LoggerErrorTypes.Database);
                success = false;
            }

            return success;
        }


        //---------------------------------------------------------------------------------------------------------------
        public bool ClearTable(string sTableName) {
            bool success = false;

            try {
                this.ExecuteSQL("DELETE FROM " + sTableName + ";");

            } catch (Exception ex) {
                Logger.LogError( thisClassName, "methodName", "Error deleting the table", ex, null, LoggerErrorTypes.Database);
                success = false;
            }

            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        //--- Public Method - WARNING - MAY ONLY WORK FOR MYSQL!!!!!
        //--- Overloaded:	Yes
        //--- Return Value:	none
        //--- Purpose: Build a table with the specified format and rows.
        /// Given a table name, a string denoting
        /// the column formats, and an array of strings denoting
        /// the row values, this method removes any existing versions of the designated
        /// table, issues a CREATE TABLE command with the
        /// designated format, then sends a series of INSERT INTO
        /// commands for each of the rows.
        public bool createTable(string sTableName, string sTableFormat) {
            return CreateTable(sTableName, sTableFormat, false);
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool CreateTable(string sTableName, string sTableFormat, bool isMemoryTable) {
            return CreateTable(sTableName, sTableFormat, isMemoryTable, false);
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///
        ///     isHugeTable allows tables to be created that are greater than 4gb in size.  This is not necessary in version 5
        ///     as the default file size in this case is 65,500TB.  In version 4 there might be performance issues with doing this for every table.
        ///
        ///     20-Jul-2016 - Note that this will automatically delete the table, if it exists!  This may not be what you require here...
        ///     Mitigated slightly by wrapping the table with a check that it actually exists...
        /// </summary>
        public bool CreateTable(string sTableName, string sTableFormat, bool isMemoryTable, bool isHugeTable) {
            bool success = false;
            string createTableSQL = "CREATE TABLE " + sTableName + " (" + sTableFormat + " )";
            if (isMemoryTable) {
                createTableSQL = createTableSQL + " ENGINE=HEAP";
            }
            //Logger.Log(createTableSQL);

            try {
                if ( TableExists(sTableName) == true) { 
                    DeleteTable(sTableName);
                }

                this.ExecuteNonQuery(createTableSQL);
                success = true;
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "Create Table", "Error Creating the table", ex, createTableSQL, LoggerErrorTypes.Database);
            }

            // alter the table, so that the max row size is huge, if needed
            // http://jeremy.zawodny.com/blog/archives/000796.html you beauty
            // show table status like 'coords_generalsurfaces';
            if (success && isHugeTable) {
                ConvertToHugeTable(sTableName);
            }

            return success;
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------

        /// <summary>
        /// Copies items where the begining of the aFieldName column matches a string inside aSearchFronts into a new table called aDestTableName.
        /// </summary>
        /// <param name="aSourceTableName">Source for items.</param>
        /// <param name="aDestTableName">This table is created if it does not exist.</param>
        /// <param name="aFieldName">String field name to search e.g. EnumerationDistrict</param>
        /// <param name="aSearchFronts">List of string fronts. E.g. {"00NA","30UH"}</param>
        /// <param name="aLCF">Load config file for DB access.</param>
        /// <returns>True if succeded.</returns>
        //public bool SplitOnFieldFront(string aSourceTableName, string aDestTableName, string aFieldName, List<string> aSearchFronts, ConfigurationInfo aLCF) {
        //    bool tWorked = true;
        //    DatabaseWrapper dbInfo = null;
        //    string tSQL = "";
        //    try {
        //        if (aSearchFronts.Count <= 0) {
        //            tWorked = false;
        //            Logger.LogError(6, "Split list empty");
        //        }

        //        dbInfo = new DatabaseWrapper(aLCF);

        //        if (!dbInfo.TableExists(aSourceTableName)) {
        //            tWorked = false;
        //            Logger.LogError(6, aSourceTableName + " can not be split as it can't be found");
        //        }

        //        if (tWorked) {
        //            if (!dbInfo.ColumnExists(aSourceTableName, aFieldName)) {
        //                tWorked = false;
        //                Logger.LogError(6, "Column " + aFieldName + " does not exist in " + aSourceTableName);
        //            }
        //        }

        //        if (tWorked) {
        //            string tType = dbInfo.GetColumnType(aSourceTableName, aFieldName);
        //            if (!tType.ToUpperInvariant().Contains("VARCHAR")) {
        //                tWorked = false;
        //                Logger.LogError(6, "Column " + aFieldName + " must be a string for split");
        //            }
        //        }

        //        if (tWorked) {
        //            if (!dbInfo.TableExists(aDestTableName)) {
        //                tSQL = "CREATE TABLE " + aDestTableName + " LIKE " + aSourceTableName + ";";
        //                dbInfo.ExecuteSQL(tSQL, ref tWorked);
        //            }
        //        }

        //        if (tWorked) {
        //            tSQL = "INSERT INTO " + aDestTableName + "\r\n";
        //            tSQL += "SELECT * FROM " + aSourceTableName + "\r\n";
        //            tSQL += "WHERE ";
        //            string tFront = "";
        //            for (int i = 0; i < aSearchFronts.Count; i++) {
        //                tFront = aSearchFronts[i];
        //                if (tFront != null && tFront != "") {
        //                    tSQL += aFieldName + " LIKE '" + tFront + "%'";
        //                    if (i < aSearchFronts.Count - 1) {
        //                        tSQL += "\r\nOR ";
        //                    }
        //                } else {
        //                    Logger.LogWarning("Empty split value " + i.ToString() + " skipped");
        //                }
        //            }

        //            //If last split string was empty
        //            if (tSQL.EndsWith("OR")) {
        //                tSQL = tSQL.Remove(tSQL.Length - "OR".Length);
        //            }

        //            tSQL += ";";

        //            int tDone = dbInfo.ExecuteSQL(tSQL, ref tWorked);
        //            Logger.Log(tDone.ToString() + " added to " + aDestTableName);
        //        }
        //    } catch (Exception ex) {
        //        Logger.LogError(5, "DatabaseInformation.SplitOnFieldFront crashed when access was attempted: " + ex.ToString());
        //        tWorked = false;
        //    } finally {
        //        if (dbInfo != null) {
        //            dbInfo.Disconnect();
        //        }
        //    }
        //    return tWorked;
        //}

        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool ConvertToHugeTable(string tableName) {
            bool success = false;

            //            Logger.Log("Converting table "+tableName+" to be potentially huge (to support up to 4.2 billion records)");
            string alterSQL = "ALTER TABLE " + tableName + " max_rows = 200000000000  avg_row_length = 50;";
            try {
                if (TableExists(tableName)) {
                    this.ExecuteNonQuery(alterSQL);
                }
                success = true;
            } catch (Exception ex) {
                Logger.LogError( thisClassName, "ConvertToHugeTable", null, ex, alterSQL, LoggerErrorTypes.Database);
            }

            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// This will make a copy and apply all the indices.
        /// </summary>
        /// <param name="SourceTable"></param>
        /// <param name="DestinationTable"></param>
        /// <returns></returns>
        public bool TryMakeCopyOfATable(string SourceTable, string DestinationTable) {
            return TryMakeCopyOfATable(SourceTable, DestinationTable, true);
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// This Method will create a copy of a given table (both structure and Data);
        /// In the Second Stage it creates the Required Indices.
        /// </summary>
        /// <param name="SourceTable"></param>
        /// <param name="DestinationTable"></param>
        /// <returns></returns>
        public bool TryMakeCopyOfATable(string SourceTable, string DestinationTable, bool createIndex) {
            bool success = false;

            Logger.Log("Copying Table " + SourceTable + " with both structure and data into " + DestinationTable + ":");
            string SQLStatement = "CREATE TABLE IF NOT EXISTS " +
                                   DestinationTable + " SELECT * FROM " +
                                   SourceTable + ";";
            try {
                if (TableExists(DestinationTable)) {
                    DropTable(DestinationTable);
                }
                Logger.Log("Copying table and time will vary according to size of source table");
                this.ExecuteNonQuery(SQLStatement);

                if (createIndex) {
                    Dictionary<string, ColumnIndexDescriptor> colAndIndices = TryGetColsAndIndiciesFromTable(SourceTable);
                    //Looping through the column info and making a SQL Statement to alter table
                    Logger.Log("Getting the indicies information for table : " + DestinationTable + ":");
                    Logger.Log("There are : " + colAndIndices.Count + " columns to be Indexed");
                    string sql = TryGetSQLForIndices(DestinationTable, colAndIndices);

                    if (DestinationTable != null && sql != string.Empty) {
                        try {
                            Logger.Log("Started Indexing " + colAndIndices.Count + " columns int Table : " + DestinationTable + ".");
                            Logger.Log("This Process will take time according to the size of the table.");
                            ExecuteSQL(sql, ref success);
                            Logger.Log("Finished Indexing : " + colAndIndices.Count + " columns int Table : " + DestinationTable + ".");

                        } catch (Exception ex2) {
                            string methodName = "TryMakeCopyOfATable";
                            Logger.LogError(thisClassName, methodName, "Error when Indexing", ex2, sql, LoggerErrorTypes.Database);
                        }
                    }
                }
            } catch (Exception ex) {

                Logger.LogError(thisClassName, "TryMakeCopyOfATable", "Error when Indexing", ex, SQLStatement, LoggerErrorTypes.Database);
            }
            return success;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Given a Table, return the Fields which are indexed, theIndexName and indexType
        /// </summary>
        /// <param name="TableName"></param>
        /// <returns></returns>
        public Dictionary<string, ColumnIndexDescriptor> TryGetColsAndIndiciesFromTable(string TableName) {
            Dictionary<string, ColumnIndexDescriptor> result = new Dictionary<string, ColumnIndexDescriptor>();
            ColumnIndexDescriptor colIdxDesc;
            string sqlStatement = "show index from " + TableName + ";";
            IDataReader dataReader = null;

            try {
                Logger.Log("Reading table decriptive information to extract indices information...:");
                if (!TableExists(TableName)) {
                    Logger.LogWarning("The Main Alias COU Table " + TableName + " does not exist.");
                    return result;
                }

                dataReader = this.RunSqlReader(sqlStatement);

                while (dataReader.Read()) {
                    try {
                        string columnName = dataReader["Column_name"].ToString();
                        string keyName = dataReader["Key_name"].ToString();
                        string indexType = dataReader["Index_type"].ToString();

                        colIdxDesc = new ColumnIndexDescriptor(columnName, keyName, indexType);

                        if (!result.ContainsKey(columnName))
                            result.Add(columnName, colIdxDesc);
                        else
                            Logger.LogWarning("Column " + columnName + " appears to have more than one index. Please check the table!");

                    } catch (Exception exp) {
                        string msg = "Failed to read Indexed Column Information from Table "
                                     + TableName + " :" + exp.StackTrace;
                        Logger.LogError(6, msg);
                        throw new Exception(msg, exp);
                    }
                }
            } catch (Exception ex) {
                string msg = "Failed to crete a list of columns and corresponding indices "
                            + TableName + " :" + ex.StackTrace;
                Logger.LogError(6, msg);
                throw new Exception(msg, ex);
            } finally {
                if (dataReader != null && !dataReader.IsClosed) {
                    dataReader.Close();
                }
            }
            return result;
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// This Method Reads the Column Information ColumnIndexDescriptor and creates a SQL Statement to add all the Indices at once.
        /// It Assumes that no column is indexed previously. This method is writtes to suooprt Copy Table SQL Statement in which
        /// data structure and data are copied to new table, however indices are not set.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnsAndIndices"></param>
        /// <returns></returns>
        public string TryGetSQLForIndices(string tableName, Dictionary<string, ColumnIndexDescriptor> columnsAndIndices) {
            StringBuilder sql = new StringBuilder();
            string partSQL = string.Empty;
            sql.Append("ALTER TABLE ");
            sql.Append(tableName);

            int i = 0;
            foreach (ColumnIndexDescriptor col in columnsAndIndices.Values) {
                //col.ColumnName;
                //col.IndexType;
                //col.KeyName;
                if (col.ColumnName != null && col.ColumnName != String.Empty) {
                    if (col.KeyName.ToLower().Contains("primary")) {
                        partSQL = " ADD PRIMARY KEY `";
                        partSQL = partSQL + col.ColumnName;
                    } else if (col.IndexType.ToLower().Contains("spatial")) {
                        partSQL = " ADD SPATIAL `";
                    } else {
                        partSQL = " ADD INDEX `";
                    }

                    sql.Append(partSQL);


                    if (!col.KeyName.ToLower().Contains("primary")) {
                        sql.Append(col.KeyName);
                    }

                    sql.Append("`");

                    if (!col.IndexType.ToLower().Contains("spatial")) {
                        sql.Append(" USING ");
                        sql.Append(col.IndexType);
                    }
                    sql.Append(" (`");

                    if (col.KeyName.ToLower().Contains("primary")) {
                        sql.Append(col.ColumnName);
                    } else {
                        sql.Append(col.ColumnName);
                    }
                    sql.Append("`)");

                    if (i != (columnsAndIndices.Count - 1)) {
                        sql.Append(",");
                    }
                }

                i++;
            }
            sql.Append(";");

            //to do
            //Only attempt to index those columns that exists in the dest table.
            //Warn for the other
            //conside using a list instead of dict.

            //Alter Table voa_september_bristol ADD INDEX `Name_Number` USING  BTREE (`Name_Number`)
            return sql.ToString();
        }


        //--------------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<ColumnDescription> GetTableDescription(string tableName) {
            List<ColumnDescription> Result = null;
            IDataReader dbReader = null;
            string sql = "DESCRIBE " + tableName + ";";
            try {
                dbReader = this.RunSqlReader(sql);
                if (dbReader == null) {
                    Logger.LogError(6, "Error reading from table in the database when trying to 'GetTableDescription' using sql:" + sql);
                    return null;
                }
                Result = new List<ColumnDescription>();
                while (dbReader.Read()) {
                    ColumnDescription colDes = new ColumnDescription();

                    //1. Column Name, If null Error
                    if (dbReader[0] != System.DBNull.Value) {
                        colDes.ColumnName = dbReader[0].ToString();
                    } else {
                        Logger.LogError(6, "Got Null Column Type using Sql: " + sql);
                        return null;
                    }
                    //2. Column Type, If null Error
                    if (dbReader[1] != System.DBNull.Value) {
                        colDes.ColumnType = dbReader[1].ToString();
                    } else {
                        Logger.LogError(6, "Got Null Column Type using Sql: " + sql);
                        return null;
                    }
                    //3. ISNull, If empty it is a null value
                    if (dbReader[2] != System.DBNull.Value) {
                        if (dbReader[2].ToString() == string.Empty) {
                            colDes.ISNull = false;
                        } else if (dbReader[2].ToString().Equals("yes", StringComparison.CurrentCultureIgnoreCase)) {
                            colDes.ISNull = true;
                        }
                    } else {
                        Logger.LogError(6, "Got Null value for 'Null' Column using Sql: " + sql);
                        return null;
                    }

                    //5. Default Value, If empty it is a null value
                    if (dbReader[4] != System.DBNull.Value) {
                        colDes.DefaultValue = dbReader[4].ToString();
                    } else {
                        colDes.DefaultValue = "NULL";
                    }

                    //6. Extra Info, If empty it is a null value
                    if (dbReader[5] != System.DBNull.Value) {
                        colDes.Extra = dbReader[5].ToString();
                    } else {
                        Logger.LogError(6, "Got Null value for 'Null' Column using Sql: " + sql);
                        return null;
                    }

                    //4. Key Info, If empty it is a null value
                    if (dbReader[3] != System.DBNull.Value) {
                        colDes.KeyName = dbReader[3].ToString();
                    } else {
                        Logger.LogError(6, "Got Null value for 'Null' Column using Sql: " + sql);
                        return null;
                    }
                    Result.Add(colDes);
                }

            } catch (Exception ex) {
                Logger.LogError(6, "Error getting 'Table Description' using sql " + sql + " at: " + ex);
                return null;
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }
            return Result;
        }



        //------------------------------------------------------------------------------------------
        //--- Private Public
        //--- Return Value:	None
        //--- Purpose: Initialize
        //-----------------------------------------------------------------------------------------
//        public void Connect() {

            // This method used to call this  stuff below, which just sets the connection string and this is now moved to the constructor ...

//        }

        //------------------------------------------------------------------------------------------
        //--- Public Method
        //--- Return Value:	None
        //--- Purpose:		Close and destroy
        //------------------------------------------------------------------------------------------
//        public override void Disconnect() {

//            if (this.oDB != null) {
//                try {
//                    this.Disconnect();
//                } catch (Exception ex) {
//                    Logger.LogError(6, "Failed to disconnect: " + ex.StackTrace);
//                }
//            }
//        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**

            */
        //public bool IsConnected() {
        //    bool connected = false;

        //    if (this.oDB != null) {
        //        //connected = this.oDB.ValidateConnection();
        //        connected = oDB.IsConnected();
        //    }

        //    return connected;
        //}


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Legacy .... use ReadLine instead.  This will never return null</summary>
        public ArrayList readLine(int numberOfParams, string query) {
            return readLine(query);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Legacy .... use ReadLine instead.  This will never return null</summary>
        public ArrayList readLine(string query) {
            string[] temp = GetDataSingleRecord(query);
            ArrayList tempAL = new ArrayList();
            if (temp != null) {
                foreach (string tempStr in temp) {
                    tempAL.Add(tempStr);
                }
            }
            return tempAL;
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Reads a single record from the database based on the given query</summary>
        public string[] ReadLine(string query) {
            string[] data = null;
            IDataReader dbReader = null;
            try {
                dbReader = RunSqlReader(query);

                if (dbReader != null) {
                    try {

                        if (dbReader.Read()) {
                            int numberOfParams = dbReader.FieldCount;
                            data = new string[numberOfParams];
                            int i = 0;

                            while (i < numberOfParams) {
                                data[i] = dbReader.GetValue(i).ToString();
                                i++;
                            }
                        }

                        dbReader.Close();
                    } catch (Exception ex) {
                        Logger.LogError( thisClassName, "ReadLine", "", ex, query, LoggerErrorTypes.Database);
                    }
                }
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "ReadLine", "", ex, query, LoggerErrorTypes.Database);
                return null;
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }
            return data;
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Legacy .... use the overloaded method without the number of parameters.  This will never return null</summary>
        public string[] getDataSingleRecord(int numberOfParams, string query) {

            string[] temp = GetDataSingleRecord(query);
            if (temp == null) {
                temp = new string[numberOfParams];
            }

            return temp;
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Reads a single record from the database based on the given query</summary>
        public string[] GetDataSingleRecord(string query) {
            return ReadLine(query);
        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public static bool DatabaseExists(string dbName, DatabaseConnectionInfo dbConnectionInfo) {
            bool exists = false;

            //string originalDatabase = this.Database;
            //string originalConnectionString = oDB.ConnectionString;

            string tempDBN = "";
            IDataReader dbReader = null;
            DatabaseWrapper mysqlDB = null;
            try {
                // clone the DBConInfo and then change the db name to the generic mysql DB - this will therefore only work with MySQL
                //DatabaseConnectionInfo tempDBConInfo = dbConInfo.Clone();
                dbConnectionInfo.NAME = "mysql";

                mysqlDB = new DatabaseWrapper(dbConnectionInfo);

                //We need to connect to an existing database to be able to run the query
                //(otherwise the validateConnection" method fails)
                //So lets just connect to the 'mysql' database which should always be present in the database!
                // The way to do this is to temporarily set the oDB connection string to point to the 'mysql' database
                // we need to be very careful to restore the original connection in the finally block.
                // this.Database = "mysql";
                // this.oDB.ConnectionString = GetConnectionString(Server, Username, Password, Database);
                mysqlDB.Connect();
                if (mysqlDB.IsConnected() == true) {
                    dbReader = mysqlDB.RunSqlReader("SHOW DATABASES;");
                    while (dbReader.Read()) {
                        if (dbReader.IsDBNull(0) != true) {
                            tempDBN = dbReader.GetValue(0).ToString();
                        } else {
                            tempDBN = "";
                        }

                        if (tempDBN.ToLower().Equals(dbName.ToLower())) {
                            exists = true;
                            break;
                        }
                    }
                    dbReader.Close();
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Problem checking if database exists: " + ex.StackTrace);
            } finally {

                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }

                if (mysqlDB != null) {
                    mysqlDB.Disconnect();
                }

            }

            return exists;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Rename a database. Have to be careful when using this method as it try to stop and restart mySQL Services.
        /// </summary>
        /// <param name="oldDBName">Original Database Name</param>
        /// <param name="newDBName">Destination Database Name</param>
        /// <returns>True if success in renaming database and restarting mySQL</returns>
        public bool RenameDatabase(string oldDBName, string newDBName, string mySQLServiceName) {
            bool isRenamed = false;
            string mysqlServiceName = string.Empty;
            ServiceController controller = null;
            string dataPath = string.Empty;
            try {
                if (oldDBName == null || oldDBName == string.Empty) {
                    Logger.LogError(6, "Empty or null source database name is provided. Quiting.!");
                    return false;
                }
                if (DatabaseWrapper.DatabaseExists(oldDBName, dbConInfo.Clone())) {
                    Logger.LogError(6, "Source database does not exists. Quitting!");
                    return false;
                }
                if (newDBName == null || newDBName == string.Empty) {
                    Logger.LogError(6, "Empty or null destination database name is provided. Quiting.!");
                    return false;
                }
                if (DatabaseWrapper.DatabaseExists(newDBName, dbConInfo.Clone())) {
                    Logger.LogError(6, "Destination database already exists. Quitting!");
                    return false;
                }
                dataPath = GetPathOfDatabaseFolder();
                if (dataPath == null || dataPath == string.Empty) {
                    Logger.LogError(6, "Failed to get the database directory for mySQL. Quitting!");
                    return false;
                }
                controller = GetMySQLServiceController(mySQLServiceName);
                if (controller == null) {
                    Logger.LogError(6, "Failed getting mySQL Service Name. Quitting!");
                    return false;
                }
                if (controller.Status == ServiceControllerStatus.Stopped) {
                    Logger.LogError(6, "mySQL Service is not running. Can not stop. Quitting!");
                    return false;
                }
                Logger.Log("Try to stop MYSQL Service....");
                controller.Stop();
                if (!HasServiceStopped(controller)) {
                    Logger.LogError(6, "Failed to strop the service. Quitting...!");
                    return false;
                }
                Logger.Log("Service stopped. Changing the database name.");
                SimpleIO sio = new SimpleIO();
                isRenamed = sio.ChangeFolderName(dataPath, oldDBName, newDBName);
                Logger.Log("Trying to restart the service.....");
                controller.Start();
                if (HasServerStarted(controller)) {
                    Logger.Log("Successfully restarted the services.");
                    isRenamed = true;
                    Logger.Log("Trying to grant privilaes to new database...");
                    GrantPrivilegesToDatabase(newDBName);
                } else {
                    Logger.LogError(6, "Error restarting service.");
                    isRenamed = false;
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Error renaming the database at: " + ex);
                return false;
            }
            return isRenamed;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Tries to get a single variable from the mySQL show variuables command.
        /// </summary>
        /// <param name="variableName"></param>
        /// <returns></returns>
        public string GetMySqlServerVariables(string variableName) {
            string result = "";
            Dictionary<string, string> variables = GetMySqlServerVariables();

            if (variables != null && variables.Count > 0) {
                if (variables.ContainsKey(variableName)) {
                    result = variables[variableName];
                }
            }

            return result;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Returns the values of the mySQL show variables command
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, string> GetMySqlServerVariables() {
            Dictionary<string, string> result = new Dictionary<string, string>();

            try {
                List<string[]> allVariables = GetDataList("show variables;");

                if (allVariables != null && allVariables.Count > 0) {
                    foreach (string[] vars in allVariables) {
                        try {
                            if (vars[0] != null && vars[1] != null && !result.ContainsKey(vars[0])) {
                                result.Add(vars[0], vars[1]);
                            }
                        } catch (Exception ex) {
                            Logger.LogWarning("Error whilst trying to GetMySqlServerVariables(): " + ex.ToString());
                        }
                    }
                }
            } catch (Exception ex) {
                Logger.LogError(6, "there was an error whilst trying to get all the variables from mySQL server :" + ex);
            }

            return result;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public string GetPathOfDatabaseFolder() {
            string path = string.Empty;
            string sql = string.Empty;
            IDataReader reader = null;
            DatabaseWrapper mysqlDB = null;

            try {
                DatabaseConnectionInfo tempDBConInfo = dbConInfo.Clone();
                tempDBConInfo.NAME = "mysql";

                mysqlDB = new DatabaseWrapper( tempDBConInfo ); //DBProvider, Server, "mysql", Username, Password, m_sConnectionPort);
                mysqlDB.Connect();
                sql = "show variables like'%datadir%'";

                reader = mysqlDB.RunSqlReader(sql);
                if (reader == null) {
                    Logger.LogError(6, "Failed to initiate reader. Quitting");
                    return null;
                }
                while (reader.Read()) {
                    path = reader["Value"].ToString();
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Error getting the path of database folder at: " + ex);
                return null;
            } finally {
                if (mysqlDB != null)
                    mysqlDB.Disconnect();
            }
            return path;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public static bool HasServerStarted(ServiceController service) {
            bool isRunning = true;
            try {
                int i = 0;
                while (service.Status != ServiceControllerStatus.Running) {
                    if (i < 60) {
                        Thread.Sleep(1000);
                    } else {
                        isRunning = false;
                        break;
                    }
                    service.Refresh();
                    i++;
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Error checking if service :" + service.ServiceName + " is stopped: "+ex.ToString());
                return false;
            }
            return isRunning;

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public static bool HasServiceStopped(ServiceController service) {
            bool isStopped = true;
            try {
                int i = 0;
                while (service.Status != ServiceControllerStatus.Stopped) {
                    if (i < 60) {
                        Thread.Sleep(1000);
                    } else {
                        isStopped = false;
                        break;
                    }
                    service.Refresh();
                    i++;
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Error checking if service :" + service.ServiceName + " is stopped:" + ex.ToString());
                return false;
            }
            return isStopped;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public static ServiceController GetMySQLServiceController(string mySQLServiceName) {

            ServiceController mySQLService = null;
            ServiceController[] services = null;
            if (mySQLServiceName == null || mySQLServiceName == string.Empty) {
                Logger.LogWarning("MySQL Service Name is not provided. Using default name!");
                mySQLServiceName = "mysql";
            } else {
                mySQLServiceName = mySQLServiceName.ToLower();
            }
            try {
                // get list of Windows services
                services = ServiceController.GetServices();
                if (services == null || services.Length < 1) {
                    Logger.LogError(6, "Failed to get the list or services running.");
                    return null;
                }
                foreach (ServiceController service in services) {
                    if (service.ServiceName.ToLower().Contains(mySQLServiceName)) {
                        mySQLService = service;
                        break;
                    }
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Error getting mySQL service Name at: " + ex);
                return null;
            }
            return mySQLService;

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool StopMySQLService(string serviceName) {
            bool isStopped = false;
            ServiceController controller = null;
            try {
                controller = GetMySQLServiceController(serviceName);
                if (controller == null) {
                    Logger.LogError(6, "Failed getting service controller for mySQL. Quitting!");
                    return false;
                }
                if (controller.Status == ServiceControllerStatus.Stopped) {
                    Logger.LogError(6, "mySQL Service is not running. Can not stop it. Quitting!");
                    return false;
                }
                Logger.Log("Try to stop MYSQL Service....");
                controller.Stop();
                isStopped = HasServiceStopped(controller);
                if (!isStopped) {
                    Logger.LogError(6, "Failed to strop the service. Quitting...!");
                } else {
                    Logger.Log("Service : " + serviceName + " has been stopped....!");
                    isStopped = true;
                }
            } catch (Exception ex) {
                Logger.Log("Error stopping mySQL Service: " + serviceName + " at: " + ex.ToString());
                isStopped = false;
            }
            return isStopped;

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool StartMySQLService(string serviceName) {
            bool isStarted = false;
            ServiceController controller = null;
            try {
                controller = GetMySQLServiceController(serviceName);
                if (controller == null) {
                    Logger.LogError(6, "Failed getting service controller for mySQL. Quitting!");
                    return false;
                }
                if (controller.Status == ServiceControllerStatus.Running) {
                    Logger.LogError(6, "mySQL Service is already running!");
                    return false;
                }
                Logger.Log("Try to start MYSQL Service....");
                controller.Start();
                isStarted = HasServerStarted(controller);
                if (!isStarted) {
                    Logger.LogError(6, "Failed to start the service. Quitting...!");
                } else {
                    Logger.Log("Service : " + serviceName + " has been started....!");
                    isStarted = true;
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Error starting mySQL Service: " + serviceName + " at: " + ex.ToString());
                isStarted = false;
            }
            return isStarted;

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// <b>CAUTION!!!</b> THIS method only checks for table exists in
        /// the database used to build the DatabaseInformation instance.
        /// i.e. dont pass in elevate_staging.tablename!!! cos this method
        /// wont work!! :D
        /// Use the overloaded version with 2 input string instead!
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool TableExists(string tableName) {

            string tempDBName = null;
            string tempTN = tableName;

            try {
                string[] bits = tableName.Split('.');
                if (bits.Length > 1) {
                    tempDBName = bits[0];
                    tempTN = bits[1];
                }
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "TableExists", "", ex, null, LoggerErrorTypes.Database);

            }

            return TableExists(tempDBName, tempTN);
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// This method uses SHOW TABLE from DBName;
        /// So you can check for table exists in<b> ANOTHER</b> database
        /// rather than just in the one used to construct this
        /// instance of DatabaseInformation
        /// </summary>
        /// <param name="dbName">Set this to "" to just return the
        /// table exists for this database information's database</param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool TableExists(string dbName, string tableName) {
            bool exists = false;

            //if (this.oDB != null) {
                string showTablesQuery = "";

                if (dbName == null || dbName == "") {
                    showTablesQuery = "SHOW TABLES;";
                } else {
                    showTablesQuery = "SHOW TABLES from " + dbName + ";";
                }

                IDataReader dbReader = RunSqlReader(showTablesQuery);

                string tempTN = "";

                // read the dbReader!
                try {

                    while (dbReader.Read()) {

                        if (dbReader.IsDBNull(0) != true) {
                            tempTN = dbReader.GetValue(0).ToString();
                        } else {
                            tempTN = "";
                        }
                        //Logger.Log(tempTN + "      " + tableName);
                        if (tempTN.ToLower().Equals(tableName.ToLower())) {
                            exists = true;
                            break;
                        }
                    }
                    //Close the reader
                    dbReader.Close();

                } catch (Exception exp) {
                    string sError = exp.Message;
                } finally {
                    //					myDB.Disconnect();
                }
            //}

            return exists;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            This is MySQL Specific - generates this sort of thing:
            LOAD DATA INFILE './SSBO.txt' INTO TABLE xxx_sbo FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n';
        */
        public string GenerateInsertSQLString(ref string fileName, ref string tableName, ref string endOfLineChar, ref string separator, bool stringsDoubleQuoted, bool loadAutoIncrementFields) {
            string sql = "";

            sql = "LOAD DATA INFILE './" + fileName + "' INTO TABLE " + tableName + " FIELDS TERMINATED BY '" + separator + "'";

            if (stringsDoubleQuoted == true) {
                sql = sql + " OPTIONALLY ENCLOSED BY '\"'";
            }
            sql = sql + " LINES TERMINATED BY '" + endOfLineChar + "'";

            string nonAutoFields = "";
            // check to see if there are any auto_increment fields and do not load this data if this is the case
            if (loadAutoIncrementFields == false) {
                // get the names of all of the non auto_increment fields
                // Field, Type, Null, Key, Default, Extra

                IDataReader dbReader = null;

                try {

                    dbReader = this.RunSqlReader("DESCRIBE " + tableName + ";");

                    string temp = "";

                    while (dbReader.Read()) {
                        temp = dbReader.GetValue(5).ToString();
                        temp = temp.ToLower();

                        if (temp.Equals("auto_increment") == false) {
                            temp = dbReader.GetValue(0).ToString();
                            nonAutoFields = (nonAutoFields != "") ? nonAutoFields + ", " + temp : temp;
                        }
                    }

                    if (nonAutoFields != "") {
                        nonAutoFields = " ( " + nonAutoFields + " ) ";
                    }

                    sql = sql + nonAutoFields;

                    dbReader.Close();

                } catch (Exception ex) {
                    Logger.LogError(thisClassName, "GenerateInsertSQLString", "", ex, "", LoggerErrorTypes.Database);
                } finally {

                    if (dbReader != null && !dbReader.IsClosed) {
                        dbReader.Close();
                    }

                }
            }

            sql = sql + ";";
            return sql;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            Load the data

            We can create a version of this that is more flexible as well
        */
        public bool LoadData(ref string fileName, ref string tableName, string endOfLineChar, string separator, bool stringsDoubleQuoted, bool loadAutoIncrementFields) {
            bool loaded = false;

            string loadDataSQL = GenerateInsertSQLString(ref fileName, ref tableName, ref endOfLineChar, ref separator, stringsDoubleQuoted, loadAutoIncrementFields);
            int numLoaded = this.ExecuteSQL(loadDataSQL, ref loaded);

            // check for errors - done by execute update...
            loaded = loaded && (numLoaded > 0);

            return loaded;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        ///<summary>
        /// This is MySQL specific
        ///</summary>
        public bool CopyTable(ref string sourceTable, ref string destinationTable) {
            return CopyTable(sourceTable, destinationTable, false, String.Empty);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool CopyTable(string sourceTable, string destinationTable) {
            return CopyTable(sourceTable, destinationTable, false, String.Empty);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool CopyTable(string sourceTable, string destinationTable, bool isSchemaOnly) {
            return CopyTable(sourceTable, destinationTable, isSchemaOnly, String.Empty);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool CopyTable(string sourceTable, string destinationTable, bool isSchemaOnly, string whereConditions) {
            return CopyTable(sourceTable, destinationTable, isSchemaOnly, whereConditions, false);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool CopyTable(string sourceTable, string destinationTable, bool isSchemaOnly, string whereConditions, bool isHuge) {
            bool useShowCreateTable = false;
            string[] insertDateFromTheseCols = null;

            return CopyTable(sourceTable, destinationTable, isSchemaOnly, whereConditions, isHuge, useShowCreateTable, insertDateFromTheseCols);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///         Copies the given table to the destination table, and only includes the given column names
        ///         Note that the resultant table will ONLY include the column names provided
        /// </summary>
        public bool CopyTable(string sourceTable, string destinationTable, List<string> onlyIncludeTheseColumnNames) {
            bool copied = false;

            if (onlyIncludeTheseColumnNames == null || onlyIncludeTheseColumnNames.Count == 0) {
                return CopyTable(sourceTable, destinationTable);
            } else {

                string paramList = this.GetTableColumnParameterList(ref sourceTable);
                bool tableCreated = this.createTable(destinationTable, paramList);

                // ensure the given column names are all lower case and then remove all other irrelevant columns
                if (tableCreated) {
                    for (int i = 0; i < onlyIncludeTheseColumnNames.Count; i++) {
                        onlyIncludeTheseColumnNames[i] = onlyIncludeTheseColumnNames[i].ToLower();
                    }

                    List<string> sourceColList = GetColumnNames(sourceTable);
                    foreach (string sourceCol in sourceColList) {
                        if (onlyIncludeTheseColumnNames.Contains(sourceCol.ToLower()) == false) {
                            ExecuteSQL("ALTER TABLE " + destinationTable + " DROP " + sourceCol + ";", ref copied);
                        }
                    }

                    // now insert the data into this table, but only for the relevant columns ...
                    string insertSQL = "INSERT INTO " + destinationTable + "(" + DataUtilities.GetCSVList(onlyIncludeTheseColumnNames, false) + ") SELECT " + DataUtilities.GetCSVList(onlyIncludeTheseColumnNames, false) + " FROM " + sourceTable + ";";
                    int numRecordsInserted = this.ExecuteSQL(insertSQL, ref copied);

                    copied = copied & (numRecordsInserted > 0);
                }
            }
            return copied;
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool CopyTable(string sourceTable, string destinationTable, bool isSchemaOnly, string whereConditions, bool isHuge, bool useShowCreateTable) {
            return CopyTable(sourceTable, destinationTable, isSchemaOnly, whereConditions, isHuge, useShowCreateTable, null);
        }
        ///-----------------------------------------------------------------------------------
        public bool CopyTable(string sourceTable, string destinationTable, bool isSchemaOnly, string whereConditions, bool isHuge, bool useShowCreateTable, string[] insertDataFromTheseColumns) {
            bool copied = false;
            bool tableCreated = false;
            try {
                if (useShowCreateTable) {
                    string createTableText = "";

                    Logger.Log("Constructing a create table SQL for destination table by using the definition to source table.");
                    createTableText = this.ReadLine("SHOW CREATE TABLE " + sourceTable + ";")[1];
                    if (createTableText == null || createTableText == string.Empty) {
                        Logger.LogError(6, "Failed to get the table definition from source table. Can not continue. Quitting.");
                        return false;
                    }
                    createTableText = createTableText.ToLower();
                    // Remove the funny quotes as it doesnt work with these
                    createTableText = createTableText.Replace("`" + sourceTable.ToLower() + "`", destinationTable.ToLower());
                    // special case if the database name is not included, add it
                    createTableText = createTableText.Replace("`" + this.dbConInfo.NAME.ToLower() + "." + sourceTable.ToLower() + "`", destinationTable.ToLower());
                    // special case the other way - replace the database name prefix in the Copy Table command, if it had one ...
                    string tempSTN = sourceTable.ToLower().Replace(this.dbConInfo.NAME.ToLower() + ".", "");
                    createTableText = createTableText.Replace("`" + tempSTN + "`", destinationTable.ToLower());

                    Logger.Log("Dropping the destination tbale :" + destinationTable + " if already exists.");
                    tableCreated = EnsureTableCreated(destinationTable, createTableText);

                } else {
                    Logger.Log("Trying to create table using parameter list supplied.");
                    tableCreated = EnsureTableCreatedWithParameterList(sourceTable, destinationTable, false, isHuge);
                    if (!tableCreated) {
                        Logger.LogError(6, "Failed to create table using non show create table command!");
                    }
                }


                if (isHuge) {
                    Logger.Log("Ensuring that table is converted to 'Huge'");
                    this.ConvertToHugeTable(destinationTable);
                }

                if (tableCreated && !isSchemaOnly) {
                    Logger.Log("Destination empty table is created and is not schema only. Therefore inserting the data into destination table.");
                    string insertSQL = string.Empty;
                    string colList = string.Empty;
                    if (insertDataFromTheseColumns != null && insertDataFromTheseColumns.Length > 0) {
                        Logger.Log("Inserting the data into destination table using only the list of column/s supplied.");
                        colList = DataUtilities.GetCSVList(insertDataFromTheseColumns);
                        if (colList == null || colList == string.Empty) {
                            Logger.LogError(6, "Failed to get the column list into a string in a proper format. Quitting.");
                            return false;
                        } else {
                            insertSQL = "INSERT INTO " + destinationTable + "(" + colList + ") SELECT " + colList + " FROM " + sourceTable;
                        }
                    } else {
                        insertSQL = "INSERT INTO " + destinationTable + " SELECT * FROM " + sourceTable;
                    }
                    if (whereConditions != null && whereConditions != String.Empty) {
                        Logger.Log("Where condition is supplied therefore appending it to insert main statement.");
                        insertSQL += " WHERE " + whereConditions;
                    }
                    Logger.Log("Trying to run the SQL.......!");
                    int numRecordsInserted = this.ExecuteSQL(insertSQL, ref copied);
                    if (numRecordsInserted > 0 || copied == false) {
                        copied = true;
                    }
                } else {
                    Logger.Log("Only an empty table is created.");
                    copied = tableCreated;
                }
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "CopyTable - Using Show Create Table", "", ex, "", LoggerErrorTypes.Database);
            }
            return copied;
        }

        //----------------------------------------------------------------------------------------------------------------------------------------------------------
        private bool EnsureTableCreated(string tableName, string createSQL) {
            bool isSuccess = false;
            try {
                if (this.TableExists(tableName)) {
                    Logger.Log("Table : " + tableName + " exists. Trying to drop it.");
                    if (this.DropTable(tableName)) {
                        Logger.Log("Successfully dropped the table: " + tableName);
                    } else {
                        Logger.LogError(6, "Failed to drop table :" + tableName + " . Quitting..!");
                        return false;
                    }
                }
                Logger.Log("Trying to create the table :" + tableName);
                this.ExecuteSQL(createSQL, ref isSuccess);
                isSuccess = isSuccess & this.TableExists(tableName);
            } catch (Exception ex) {
                Logger.LogError(6, "Error ensuring the table creation at: " + ex);
                isSuccess = false;
            }
            return isSuccess;
        }

        //----------------------------------------------------------------------------------------------------------------------------------------------------------
        private bool EnsureTableCreatedWithParameterList(string srcTable, string destTable, bool isMemoryTable, bool isHuge) {
            bool isCreated = false;
            string paramList = string.Empty;
            try {
                paramList = this.GetTableColumnParameterList(ref srcTable);
                if (paramList == null || paramList == string.Empty) {
                    Logger.LogError(6, "Null or empty parameter list found. Quitting..!");
                    return false;
                }
                if (this.TableExists(destTable)) {
                    Logger.Log("Table : " + destTable + " exists. Trying to drop it.");
                    if (this.DropTable(destTable)) {
                        Logger.Log("Successfully dropped the table: " + destTable);
                    } else {
                        Logger.LogError(6, "Failed to drop table :" + destTable + " . Quitting..!");
                        return false;
                    }
                }
                Logger.Log("Trying to create the table :" + destTable);
                isCreated = this.CreateTable(destTable, paramList, isMemoryTable, isHuge);
            } catch (Exception ex) {
                Logger.LogError(6, "Error creating table with parameter list at :" + ex);
                isCreated = false;
            }
            return isCreated;
        }

        /// <summary>
        /// Returns a list of fields that are common to both tables excuding ID.
        /// </summary>
        /// <param name="tableA"></param>
        /// <param name="tableB"></param>
        /// <returns>List of field names that are common to both tables.</returns>
        public string[] GetColNameCSVCommonExcludingID(string tableA, string tableB) {
            List<string> tListA = new List<string>(GetColumnNames(tableA, new List<string>(new string[] { "ID" }), String.Empty));
            List<string> tListB = new List<string>(GetColumnNames(tableB, new List<string>(new string[] { "ID" }), String.Empty));
            List<string> tListComp = new List<string>();

            Logger.Log("About to compair field lists from " + tableA + " to " + tableB);

            bool tFound = false;
            foreach (string tField in tListA) {
                foreach (string tCompField in tListB) {
                    if (tField.Equals(tCompField, StringComparison.CurrentCultureIgnoreCase)) {
                        tListComp.Add(tField);
                        tFound = true;
                        break;
                    }
                }
                if (!tFound) {
                    Logger.Log("Field from from " + tableA + " called " + tField + " not found in " + tableB);
                }
            }

            //Report loop only. Full intersect of lists was found in first loop.
            foreach (string tField in tListB) {
                foreach (string tCompField in tListA) {
                    if (tField.Equals(tCompField, StringComparison.CurrentCultureIgnoreCase)) {
                        tFound = true;
                        break;
                    }
                }
                if (!tFound) {
                    Logger.Log("Field from from " + tableB + " called " + tField + " not found in " + tableA);
                }
            }

            //log
            return tListComp.ToArray();
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public string[] GetColNameCSVExcludingID(string table) {
            return GetColumnNames(table, new List<string>(new string[] { "ID" }), String.Empty);
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public string[] GetColNameCSVExcludingID(string table, string tableAlias) {
            return GetColumnNames(table, new List<string>(new string[] { "ID" }), tableAlias);
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            This is MySQL specific
        */
        public string GetTableColumnParameterList(ref string sourceTable) {
            string parameters = "";
            IDataReader dbReader = null;
            try {


                dbReader = this.RunSqlReader("DESCRIBE " + sourceTable + ";");

                // Field, Type, Null, Key, Default, Extra
                string field = "";
                string type = "";
                string nullField = "";
                string key = "";
                string defaultValue = "";
                string extra = "";

                // 3 December 2010 - Ensure that we manage multiple primary keys effectively ...
                List<string> primaryKeyFields = new List<string>();

                while (dbReader.Read()) {
                    field = dbReader.GetValue(0).ToString();
                    type = dbReader.GetValue(1).ToString();
                    nullField = dbReader.GetValue(2).ToString();
                    key = dbReader.GetValue(3).ToString();
                    defaultValue = dbReader.GetValue(4).ToString();
                    extra = dbReader.GetValue(5).ToString();

                    parameters = (parameters != "") ? parameters + ", " + field + " " + type : field + " " + type;

                    if (nullField == "") {
                        parameters = parameters + " NOT NULL";
                    }

                    if (defaultValue != "") {
                        parameters = parameters + " DEFAULT '" + defaultValue + "'";
                    }

                    if (extra != "") {
                        parameters = parameters + " " + extra;
                    }

                    key = key.ToLower();

                    if (key.Equals("mul")) {
                        if (type.Equals("geometry", StringComparison.CurrentCultureIgnoreCase))
                            parameters = parameters + " NOT NULL , SPATIAL INDEX (" + field + ")";
                        else if (type.Equals("blob", StringComparison.CurrentCultureIgnoreCase) ||
                            type.ToLower().Contains("text"))
                            parameters = parameters + ", KEY " + field + " (" + field + "(255))";
                        else
                            parameters = parameters + ", KEY " + field + " (" + field + ")";
                    } else if (key.Equals("pri")) {
                        // 3 December 2010 - Ensure that we manage multiple primary keys effectively ...
                        //parameters = parameters + ", PRIMARY KEY (" + field + ")";
                        primaryKeyFields.Add(field);
                    }

                }


                // 3 December 2010 - Ensure that we manage multiple primary keys effectively ...
                if (primaryKeyFields != null && primaryKeyFields.Count > 0) {
                    parameters = parameters + ", PRIMARY KEY (" + DataUtilities.GetCSVList(primaryKeyFields, false) + ")";
                }


                dbReader.Close();

            } catch (Exception ex) {
                string methodName = "GetTableColumnParameterList";
                Logger.LogError(thisClassName, methodName, "Failed to get column parameter list", ex, "", LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }

            }

            return parameters;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public int ExecuteSQL(string sql, bool isSetBigSelects, ref bool success) {
            return ExecuteSQL(sql, isSetBigSelects, false, ref success);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public int ExecuteSQL(string sql, bool isSetBigSelects, bool isSuppressNoUpdateWarnings, ref bool success) {
            int updateNum = 0;

            if (!isSetBigSelects) // If not set to use big selects, ExecuteSQL as normal.
                return ExecuteSQL(sql, ref success);

            // otherwise turn on big selects, execute, check then turn off big selects.
            try {

                TurnOnSqlBigSelects();

                updateNum = ExecuteSQL(sql, ref success);
                if (updateNum == 0 && !isSuppressNoUpdateWarnings) {
                    Logger.LogWarning("Executing update with big selects turned on resulted in ZERO (0) updates! Please check this is correct for SQL:\n " + sql);
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Problem executing update with isSetBigSelects = " + isSetBigSelects + " at: " + ex);
            } finally {
                TurnOffSqlBigSelects();
            }

            return updateNum;
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public int ExecuteSQL(string sql, ref bool success) {

            //Changed to log the time if any sql update query is taking more than 2H.
            //Date: 23rd July 2009
            //Author: Maz
            // time this process

            TimeSpan start = new TimeSpan(DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second, DateTime.Now.Millisecond);

            int numAffected = 0;
            try {
                numAffected = this.ExecuteSQL(sql);

                TimeSpan end = new TimeSpan(DateTime.Now.Day, DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second, DateTime.Now.Millisecond);
                TimeSpan difference = end.Subtract(start);

                if (difference.TotalHours > 3) {
                    Logger.LogWarning("It took more than 3 Hours for a SQL Update to finish.....");
                    Logger.LogWarning("The sql update statement : " + sql + " took : " + difference.TotalHours + " hours or " + difference.TotalMinutes + " minutes to complete...!");
                }

                // got to here, then the query hasn't failed .....
                success = true;
            } catch (Exception ex) {
                success = false;
                string methodName = "ExecuteSQL";
                Logger.LogError( thisClassName, methodName, "Error running the SQL statement", ex, sql, LoggerErrorTypes.Database);
            }

            return numAffected;
        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Turns off sql big selects.
        /// </summary>
        public void TurnOnSqlBigSelects() {
            try {
                int numSet = (int)this.ExecuteNonQuery(TURN_ON_SQL_BIG_SELECTS);
            } catch (Exception ex) {
                Logger.LogError(6, "Problem turning on SQL big selects at:\n " + ex.StackTrace);
            }
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Turns on sql big selects.
        /// </summary>
        public void TurnOffSqlBigSelects() {
            try {
                int numSet = (int)this.ExecuteNonQuery(TURN_OFF_SQL_BIG_SELECTS);
            } catch (Exception ex) {
                Logger.LogError(6, "Problem turning off SQL big selects at:\n " + ex.StackTrace);
            }
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool RenameTable(string srcTablename, string destTableName) {
            bool isRenamed = false;
            string updateSQL = null;

            try {

                if (!TableExists(srcTablename)) {
                    Logger.LogError(6, "Source table " + srcTablename + "to rename does not exist!");
                    return isRenamed;
                }

                if (TableExists(destTableName)) {
                    Logger.LogError(6, "Destination table " + destTableName + "to rename to already exists!");
                    return isRenamed;
                }

                updateSQL = "ALTER TABLE " + srcTablename + " RENAME TO " + destTableName + ";";
                this.ExecuteSQL(updateSQL);

                isRenamed = TableExists(destTableName) && !TableExists(srcTablename);
            } catch (Exception ex) {
                string methodName = "RenameTable";
                Logger.LogError( thisClassName, methodName, "Error altering the table", ex, updateSQL, LoggerErrorTypes.Database);
            }

            return isRenamed;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            This gets the create table SQL using "Show Create Table <tableName>"
         *
         *
         * NOTE that in versions of MySQL before 4.1.12, this returned the tableName and the SQL as a string while versions
         * 4.1.13 & 4 return the SQL as a byte array.  In later versions this has reverted to a string.  This method will not work with
         * versions 13 & 14 - choose a later version.
        */
        public string GetCreateTableSQL(string tableName) {
            string createTableSQL = "";
            IDataReader dbReader = null;
            object tempObj = new object();
            try {
                dbReader = this.RunSqlReader("SHOW CREATE TABLE " + tableName + ";");
                while (dbReader.Read()) {
                    createTableSQL = (string)dbReader.GetValue(1);
                }
                if (createTableSQL == null || createTableSQL == string.Empty) {
                    Logger.LogError(6, "Failed to get create table SQL.");
                } else {
                    Logger.Log("Found create table SQL. Converting to lower case..!");
                    createTableSQL = createTableSQL.ToLower();
                }
            } catch (Exception ex) {
                string methodName = "GetCreateTableSQL";
                Logger.LogError( thisClassName, methodName, "You are probably using an old version of MySQL - extracting SHOW CREATE TABLE as a string", ex, null, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }
            return createTableSQL;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            This is MySQL specific
        */
        public bool ColumnExists(string table, string column) {
            return ColumnExists(ref table, ref column);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool ColumnExists(ref string table, ref string column) {
            bool exists = false;

            IDataReader dbReader = null;

            try {
                column = column.ToLower().Trim();

                // This is a synomym for show fields from, so both work looking at external databases ....
                dbReader = RunSqlReader("DESCRIBE " + table + ";");

                string temp = "";

                while (dbReader.Read()) {
                    temp = dbReader.GetValue(0).ToString().ToLower().Trim();

                    if (temp.Equals(column, StringComparison.CurrentCultureIgnoreCase)) {
                        exists = true;
                    }
                }

                dbReader.Close();

            } catch (Exception ex) {
                string methodName = "ColumnExists";
                Logger.LogError( thisClassName, methodName, "Problem identifying whether or not a column exists", ex, null, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null) {
                    try {
                        dbReader.Close();
                    } catch {
                        Logger.LogError(6, "Problem closing dbReaser");
                    }
                }
            }

            return exists;
        }


        //-------------------------------------------------------------------------------------------------------------------------
        /**
            NOTE That this resets the column if it exists
        */
        public void AddColumn(string tableName, string columnName, string columnType, string defaultVal) {
            bool success = false;

            if (ColumnExists(ref tableName, ref columnName)) { // reset it
                ExecuteSQL("UPDATE " + tableName + " SET " + columnName + "=" + defaultVal + ";", ref success);
                //                Console.WriteLine( "Resetting... " );
            } else { // create it
                ExecuteSQL("ALTER TABLE " + tableName + " ADD " + columnName + " " + columnType + " DEFAULT " + defaultVal + ";", ref success);
            }
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public void AddColumn(string tableName, string columnName, string columnType) {
            bool success = false;

            if (ColumnExists(ref tableName, ref columnName)) {
                Logger.Log("Column Named: - " + columnName + " - in Table: - " + tableName + " - Already Exists.");
            } else {//create it.
                ExecuteSQL("ALTER TABLE " + tableName + " ADD " + columnName + " " + columnType + ";", ref success);
            }
        }

        //---------------------------------------------------------------------------
        public bool DropColumns(string tableName, List<string> columnsName) {
            bool deletedAll = true;
            string tSQL = "ALTER TABLE " + tableName;
            try {
                if (tableName == null || tableName == "") {
                    Logger.LogError(6, "Null or empty table name found.");
                    return false;
                }
                if (columnsName == null || columnsName.Count == 0) {
                    Logger.LogError(6, "Null or empty list of columns found.");
                    return false;
                }

                bool isFirst = true;
                foreach (string col in columnsName) {
                    if (!isFirst) {
                        tSQL += ",";
                    }
                    tSQL += " DROP COLUMN `" + col + "`";
                    isFirst = false;
                }

                tSQL += ";";
                int effect = ExecuteSQL(tSQL, ref deletedAll);
                //Check if col are there

                foreach (string col in columnsName) {
                    if (ColumnExists(tableName, col)) {
                        deletedAll = false;
                    }
                }
            } catch (Exception ex) {
                Logger.LogError(6, "Error deleting '" + columnsName.Count.ToString() + "' columns from table " + tableName + ". The specific error is:" + ex.ToString());
                return false;
            }
            return deletedAll;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool DropColumn(string tableName, string columnName) {
            bool tWorked = true;
            if (tableName == null || tableName == "" || columnName == null || columnName == "") {
                tWorked = false;
            }

            if (tWorked) {
                string tSQL = "ALTER TABLE " + tableName;
                tSQL += " DROP COLUMN " + columnName + ";";

                try {
                    ExecuteSQL(tSQL, ref tWorked);
                } catch {
                    //SQL failed. column may not have existed.
                }

                try {
                    tWorked = !ColumnExists(tableName, columnName);
                } catch {
                    //column check failed.
                    tWorked = false;
                }
            }

            return tWorked;
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool DropColumnIndex(string tableName, string columnName) {
            bool updated = false;

            string dropIndexSQL = "DROP INDEX " + columnName + " ON " + tableName + ";";
            if (tableName != null) {
                ExecuteSQL(dropIndexSQL, ref updated);
                updated = true;
            }

            return updated;
        }



        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool IsColumnNullOrEmpty(string tableName, string columnName) {
            bool isEmptyOrNull = true;
            string whereClause = columnName + " IS NOT NULL AND " + columnName + " <> ''";
            try {
                long count = GetCount(tableName, whereClause);
                if (count > 0) {
                    isEmptyOrNull = false;
                }

            } catch (Exception ex) {
                Logger.LogError(6, "Error checking if column '" + columnName + "' is null or empty in table '" + columnName + "' at: " + ex);
                return false;
            }
            return isEmptyOrNull;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool CheckForColumnIndex(string tableName, string columnName) {
            //Logger.Log("CheckForColumnIndex:" + tableName + ":" + columnName);
            // 3 Nov 2010 - Standardise so that both do the same thing!!!!
            return IndexExists(tableName, columnName);
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool IndexExists(string tableName, string columnName) {
            bool hasIndex = false;

            if (tableName != null && tableName != string.Empty) {
                IDataReader reader = RunSqlReader("SHOW INDEX IN " + tableName + ";");

                if (reader != null) {
                    try {
                        while (reader.Read()) {
                            object field = reader["Column_name"];

                            if (field != null) {
                                string fieldStr = field as string;
                                if (fieldStr.Equals(columnName, StringComparison.CurrentCultureIgnoreCase)) {
                                    hasIndex = true;
                                    break;
                                }
                            }
                        }
                    } catch (Exception ex) {
                        Logger.LogError(thisClassName, "IndexExists", "Problem getting whether or not the index exists ...", ex, "", LoggerErrorTypes.Database);

                    } finally {
                        if (reader != null && !reader.IsClosed) {
                            reader.Close();
                        }
                    }

                }
            }

            return hasIndex;
        }


        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Gets a count of the number of indexes on the table already ...
        /// </summary>
        public int IndexCount(string tableName) {
            int numIndexes = 0;

            if (tableName != null && tableName != string.Empty) {
                IDataReader reader = RunSqlReader("SHOW INDEX IN " + tableName + ";");

                if (reader != null) {
                    try {
                        while (reader.Read()) {
                            object field = reader["Column_name"];

                            if (field != null) {
                                numIndexes++;
                            }
                        }
                    } catch (Exception ex) {
                        Logger.LogError(thisClassName, "IndexCount", "Problem getting the count of indexes ...", ex, "", LoggerErrorTypes.Database);

                    } finally {
                        if (reader != null && !reader.IsClosed) {
                            reader.Close();
                        }
                    }

                }
            }

            return numIndexes;
        }




        //-------------------------------------------------------------------------------------------------------------------------
        public bool IndexColumn(string tableName, string columnName) {
            bool updated = false;

            if (tableName != null) {
                if (ExecuteSQL("CREATE INDEX " + columnName + " ON " + tableName + "(" + columnName + ");", ref updated) > 0) {
                    updated = true;
                }
            }

            return updated;
        }
        //-------------------------------------------------------------------------------------------------------------------------
        public bool IndexColumn(string tableName, string columnName, string indexType) {
            bool updated = false;

            if (tableName != null && columnName != null) {
                if (TableExists(tableName) && CheckForColumnIndex(tableName, columnName) == false) {
                    if (ExecuteSQL("CREATE INDEX " + columnName + " USING " + indexType + " ON " + tableName + "(" + columnName + ");", ref updated) > 0) {
                        updated = true;
                    }
                }
            }

            return updated;
        }





        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Legacy .... use GetData instead.  This will never return null</summary>
        public ArrayList GetData(int numberOfParams, string query) {
            return GetData(query);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public ArrayList GetData(string query) {

            ArrayList data = new ArrayList();
            IDataReader dbReader = null;

            try {

                dbReader = RunSqlReader(query);
                int counter = 0;
                int fieldCount = 0; // get this from the first row
                while (dbReader.Read()) {
                    if (counter == 0) {
                        fieldCount = dbReader.FieldCount;
                    }

                    // temp needs to be declared each time; otherwise the object contains exactly the same data for each record
                    // Why is this?? possible due to serialisation???
                    string[] temp = new string[fieldCount];

                    int i = 0;
                    while (i < fieldCount) {
                        temp[i] = dbReader.GetValue(i).ToString();
                        i++;
                    }
                    data.Add(temp);
                }
                dbReader.Close();
            } catch (Exception ex) {
                string methodName = "GetData";
                Logger.LogError( thisClassName, methodName, "Big Error Man", ex, query, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<string[]> GetDataList(string query) {


            List<string[]> data = new List<string[]>();
            IDataReader dbReader = null;

            try {
                dbReader = RunSqlReader(query);

                int counter = 0;
                int fieldCount = 0; // get this from the first row
                while (dbReader.Read()) {
                    if (counter == 0) {
                        fieldCount = dbReader.FieldCount;
                    }

                    // temp needs to be declared each time; otherwise the object contains exactly the same data for each record
                    // Why is this?? possible due to serialisation???
                    string[] temp = new string[fieldCount];

                    int i = 0;
                    while (i < fieldCount) {
                        temp[i] = dbReader.GetValue(i).ToString();

                        i++;
                    }
                    data.Add(temp);
                }
                dbReader.Close();
            } catch (Exception ex) {
                string methodName = "GetData";
                Logger.LogError(thisClassName, methodName, "Error getting the data list", ex, query, LoggerErrorTypes.Database);
                return null;
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<object[]> GetDataObjectList(string query) {

            List<object[]> data = new List<object[]>();
            IDataReader dbReader = null;

            try {
                dbReader = RunSqlReader(query);

                int counter = 0;
                int fieldCount = 0; // get this from the first row
                while (dbReader.Read()) {
                    if (counter == 0) {
                        fieldCount = dbReader.FieldCount;
                    }

                    // temp needs to be declared each time; otherwise the object contains exactly the same data for each record
                    // Why is this?? possible due to serialisation???
                    object[] temp = new object[fieldCount];

                    int i = 0;
                    while (i < fieldCount) {
                        //temp[i] = dbReader.GetValue(i).ToString();
                        temp[i] = dbReader.GetValue(i);
                        i++;
                    }
                    data.Add(temp);
                }
                dbReader.Close();
            } catch (Exception ex) {
                string methodName = "GetDataObjectList";
                Logger.LogError(thisClassName, methodName, "Error getting the data list", ex, query, LoggerErrorTypes.Database);
                return null;
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            Drop table
        */
        public bool DropTable(string tableName) {
            bool deleted = false;

            try {
                if (TableExists(tableName)) {

                    string dropTableSQL = "DROP TABLE " + tableName + ";";
                    int dt = ExecuteSQL(dropTableSQL, ref deleted);

                    if (TableExists(tableName) == false) {
                        deleted = true;
                    }
                }
            } catch (Exception ex) {
                Logger.LogError( thisClassName, "DropTable", "Error dropping the table", ex, null, LoggerErrorTypes.Database);
            }

            return deleted;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        ///<summary>An array list of longs - optimised for extracting a list of IDs from the given query</summary>
        public List<long> GetList(string query) {
            List<long> data = new List<long>();
            long id = 0;
            string temp = "";

            IDataReader dbReader = null;
            try {
                dbReader = RunSqlReader(query);

                while (dbReader.Read()) {
                    temp = dbReader.GetValue(0).ToString();
                    if (temp != "") {
                        id = long.Parse(temp);
                        data.Add(id);
                    }
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError( thisClassName, "GetList", "Problem getting the list", ex, query + "\nSpecificValue:'" + temp + "'", LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<int> GetIntegerList(string query) {
            IDataReader dbReader = null;
            List<int> data = null;
            int id = 0;

            try {
                dbReader = RunSqlReader(query);
                if (dbReader == null) {
                    Logger.LogError(6, "Error reading data from database using query " + query);
                    return null;
                }
                data = new List<int>();
                while (dbReader.Read()) {
                    string temp = dbReader.GetValue(0).ToString();
                    bool parsingSuccess = int.TryParse(temp, out id);
                    //01-May-2013 - randomly this was not == false, so that meant this method would never have worked!!!
                    if (parsingSuccess == false) {
                        Logger.LogError(6, "Error parsing interger valuee from dbReader for 'GetIntegerList(string query)'. Quiting !");
                        return null;
                    }
                    data.Add(id);
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetIntegerList", "Problem getting the list", ex, query, LoggerErrorTypes.Database);
                return null;
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }
            return data;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public string[] GetStringList(string query, bool somthingToDifferentiate) {
            List<string> data = GetStringList(query);

            if (data != null) {
                return data.ToArray();
            }

            return null;
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<string> GetStringList(string query) {

            IDataReader dbReader = null;

            List<string> data = new List<string>();
            string id = "";

            try {
                dbReader = RunSqlReader(query);
                while (dbReader.Read()) {
                    id = dbReader.GetValue(0).ToString();
                    data.Add(id);
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetStringList", "Problem getting the list", ex, query, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Limits the results returned to the number specifed by maxLength
        /// </summary>
        public List<string> GetStringList(string query, int maxLength) {

            IDataReader dbReader = null;

            List<string> data = new List<string>();
            string id = "";

            try {
                dbReader = RunSqlReader(query);
                int count = 0;
                while (dbReader.Read()) {
                    id = dbReader.GetValue(0).ToString();
                    data.Add(id);
                    if (count++ >= maxLength) {
                        break;
                    }
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError( thisClassName, "GetStringList", "Error Getting the List", ex, query, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Gets a list of doubles.  Note that if useMGLSpecialvalues is set to false, this method will fall over if it
        /// hits null values or other values that c# cannot pass to doubles ('')</summary>
        /**
            An array list of doubles
        */
        public ArrayList GetDoubleList(string query, bool useMGLSpecialValueForNulls) {

            IDataReader dbReader = null;

            ArrayList data = new ArrayList();
            double d = 0;

            try {
                dbReader = RunSqlReader(query);

                if (useMGLSpecialValueForNulls) {
                    while (dbReader.Read()) {
                        d = -999999;    // the MGL special value is -999999
                        try {
                            d = double.Parse(dbReader.GetValue(0).ToString());
                        } catch { }
                        data.Add(d);
                    }
                } else {
                    while (dbReader.Read()) {
                        d = double.Parse(dbReader.GetValue(0).ToString());
                        data.Add(d);
                    }
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetDoubleList", "Error Getting the List", ex, query, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Gets a list of doubles.</summary>
        public List<double> GetDoubleList(string query) {

            IDataReader dbReader = null;

            List<double> data = new List<double>();

            try {
                dbReader = RunSqlReader(query);

                while (dbReader.Read()) {
                    double d = double.MinValue;
                    double.TryParse(dbReader.GetValue(0).ToString(), out d);
                    if (d != double.MinValue) {
                        data.Add(d);
                    }
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetDoubleList", "Error Getting the List", ex, query, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return data;
        }




        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            Gets a count of the number of records
        */
        public long GetCount(string tableName) {

            // The count is a long integer
            IDataReader dbReader = null;

            long count = 0;

            try {
                //FIX Me. If the * was changed to the unique ID column this query would be faster.
                dbReader = RunSqlReader("SELECT COUNT(*) FROM " + tableName + ";");

                while (dbReader.Read()) {
                    count = long.Parse(dbReader.GetValue(0).ToString());
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetCount", "Error Getting the List", ex, null, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return count;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public int GetCount(string tableName, string whereString) {

            // The count is a long integer
            IDataReader dbReader = null;
            int count = 0;

            try {
                dbReader = RunSqlReader("SELECT COUNT(*) FROM " + tableName + " WHERE " + whereString + ";");

                while (dbReader.Read()) {
                    count = int.Parse(dbReader.GetValue(0).ToString());
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetCount", "Error Getting the Count", ex, whereString, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return count;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public int GetCountForSQL(string sql) {
            IDataReader dbReader = null;
            int count = -1;
            try {
                dbReader = RunSqlReader(sql);
                if (dbReader == null) {
                    Logger.LogError(6, "Error getting count for sql: " + sql);
                    count = -1;

                }
                while (dbReader.Read()) {
                    count = int.Parse(dbReader.GetValue(0).ToString());
                }

            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetCount", "Error Getting the List", ex, sql, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }
            return count;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        //This Method gets the count from a table in different database.....
        public int GetCountFromAltDb(string dbName, string tableName) {

            // The count is a long integer
            IDataReader dbReader = null;
            int count = 0;

            try {
                dbReader = RunSqlReader("SELECT COUNT(*) FROM " + dbName + "." + tableName + ";");

                while (dbReader.Read()) {

                    count = int.Parse(dbReader.GetValue(0).ToString());
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetCountFromAltDb", null, ex, null, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return count;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        //This Method gets the count from a table in different database with where clause.....
        public int GetCountFromAltDb(string dbName, string tableName, string whereString) {

            // The count is a long integer
            IDataReader dbReader = null;
            int count = 0;

            try {
                dbReader = RunSqlReader("SELECT COUNT(*) FROM " + dbName + "." + tableName + " WHERE " + whereString + ";");

                while (dbReader.Read()) {
                    count = int.Parse(dbReader.GetValue(0).ToString());
                }
                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetCountFromAltDb", null, ex, null, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return count;
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Gets the distinct count of the values for the given column, table, where clause combination ...
        /// </summary>
        //FIX me. This SQL may be faster. SELECT COUNT(DISTINCT PostCode) FROM adp_towerhamlets WHERE PostCode like 'e%';
        public int GetCountDistinct(string tableName, string columnName, string whereClause) {
            int count = 0;

            // the first of these two queries appears more complicated, but is consistently faster !!!
            //            SELECT Sum( x.one ) FROM
            //  ( SELECT (1) as one FROM EST.al2_2008jan WHERE Postcode LIKE "M20%" AND TOID <> "222" GROUP BY Postcode ) as x;

            //SELECT COUNT(DISTINCT FullAddress) FROM EST.al2_2008jan WHERE Postcode LIKE "M20%" AND TOID <> "222";
            IDataReader dbReader = null;

            try {
                if (tableName != null && tableName != "" && columnName != null && columnName != "" && ColumnExists(tableName, columnName)) {
                    string whereClauseStr = "";
                    if (whereClause != null && whereClause != "") {
                        whereClauseStr = " WHERE " + whereClause;
                    }
                    dbReader = RunSqlReader("SELECT Sum( x.one ) FROM ( SELECT (1) as one FROM " + tableName + whereClauseStr + " GROUP BY " + columnName + " ) as x;");

                    while (dbReader.Read()) {
                        count = int.Parse(dbReader.GetValue(0).ToString());
                    }
                    dbReader.Close();
                }
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetCountDistinct", null, ex, null, LoggerErrorTypes.Database);

            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return count;
        }




        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            This is MySQL specific
        */
        public string GetColumnType(string table, string column) {
            string type = "";

            IDataReader dbReader = null;

            try {

                dbReader = this.RunSqlReader("DESCRIBE " + table + ";");

                string name = "";

                while (dbReader.Read()) {
                    name = dbReader.GetValue(0).ToString();

                    if (name.Equals(column, StringComparison.CurrentCultureIgnoreCase)) {
                        type = dbReader.GetValue(1).ToString();
                        break;
                    }
                }

                dbReader.Close();

                if (type == null || type == "") {
                    Logger.LogWarning("No type found for the combination of table " + table + " and column " + column + ", what is the problem dude?...");
                }

            } catch (Exception ex) {
                Logger.LogError(thisClassName, "GetColumnType", null, ex, null, LoggerErrorTypes.Database);
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return type;
        }

        public bool IsPrimaryKey(string table, string column) {
            bool isPK = false;

            string keyType = "";

            IDataReader dbReader = null;

            try {

                dbReader = this.RunSqlReader("DESCRIBE " + table + ";");

                string name = "";

                while (dbReader.Read()) {
                    name = dbReader.GetValue(0).ToString();

                    if (name.Equals(column, StringComparison.CurrentCultureIgnoreCase)) {
                        if (dbReader[3] != System.DBNull.Value) {
                            keyType = dbReader.GetValue(3).ToString();
                            if (keyType.StartsWith("PRI", StringComparison.CurrentCultureIgnoreCase)) {
                                isPK = true;
                            } else {
                                isPK = false;
                            }
                        }

                        break;
                    }
                }

                dbReader.Close();
            } catch (Exception ex) {
                Logger.LogError(thisClassName, "IsPrimaryKey", null, ex, null, LoggerErrorTypes.Database);
                isPK = false;
            } finally {
                if (dbReader != null && !dbReader.IsClosed) {
                    dbReader.Close();
                }
            }

            return isPK;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
            This is MySQL specific
        */
        public bool CompareColumnType(string table, string column, string genericType) { // generic type should be bigint, not bigint(20)
            bool theSame = false;

            string type = GetColumnType(table, column);

            if (type != null && type.StartsWith(genericType, StringComparison.CurrentCultureIgnoreCase)) {
                theSame = true;
            }

            return theSame;
        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool OptimiseTable(string tn) {
            bool success = false;
            try {
                ExecuteSQL("OPTIMIZE TABLE " + tn + ";", ref success);

            } catch (Exception ex) {
                Logger.LogError(thisClassName, "OptimiseTable", null, ex, null, LoggerErrorTypes.Database);
            }
            return success;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool RepairTable(string tn) {
            bool success = false;
            try {
                ExecuteSQL("REPAIR TABLE " + tn + ";", ref success);

            } catch (Exception ex) {
                Logger.LogError(thisClassName, "RepairTable", null, ex, null, LoggerErrorTypes.Database);

            }
            return success;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public void OptimiseDatabase() {
            Logger.Log("\nOptimising each table in the database...");
            string[] tns = GetTableNames();
            int count = 0;
            if (tns != null) {
                foreach (string tn in tns) {
                    OptimiseTable(tn);
                    Logger.Log(count, 10, tns.Length);
                    count++;
                }
                Logger.Log(count, 1, tns.Length);
                Logger.Log("\n...Done");
            } else {
                Logger.LogError(thisClassName, "The list of table names was null, check the database connection was successful", null, null, null, LoggerErrorTypes.Database);
            }
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool ResetColumn(string tn, string cn, string defaultVal) {
            bool reset = false;

            if (tn != null) {
                if (ExecuteSQL("UPDATE " + tn + " SET `" + cn + "`=" + defaultVal + ";", ref reset) > 0) {
                    reset = true;
                }
            }
            return reset;
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool UpdateTable(string tn, string idCN, int id, string valCN, string valInfo, bool useQuotes) {
            bool updated = false;

            if (tn != null) {
                if (useQuotes == true) {
                    valInfo = "'" + valInfo + "'";
                }

                if (ExecuteSQL("UPDATE " + tn + " SET `" + valCN + "`=" + valInfo + " WHERE " + idCN + "=" + id + ";", ref updated) > 0) {
                    updated = true;
                }
            }
            return updated;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Create a new database, given rights to the identified user.
        /// </summary>
        /// <param name="database">The database to create.</param>
        /// <param name="username">The username to authorise.</param>
        /// <param name="password">The password to authorise.</param>
        /// <param name="ifNotExists">True, if DB should only be created if it doesn't exist.</param>
        public void CreateDatabase(string database, string username, string password, bool ifNotExists) {
            DatabaseWrapper mysqlDB = null;
            try {
                // clone the DBConInfo and then change the db name to the generic mysql DB - this will therefore only work with MySQL
                DatabaseConnectionInfo tempDBConInfo = dbConInfo.Clone();
                tempDBConInfo.NAME = "mysql";

                mysqlDB = new DatabaseWrapper(tempDBConInfo);

                string sql =
                    "CREATE DATABASE " + (ifNotExists ? " IF NOT EXISTS " : String.Empty) + database + @";
	                GRANT ALL PRIVILEGES ON " + database + ".* TO " + username + "@localhost IDENTIFIED BY '" + password + @"';
	                GRANT ALL PRIVILEGES ON " + database + ".* TO " + username + "@\"%\" IDENTIFIED BY '" + password + @"';
	                FLUSH PRIVILEGES;
	                ";

                //We need to connect to an existing database to be able to run the query
                //(otherwise the validateConnection" method fails)
                //So lets just connect to the 'mysql' table which should always be present in the database!

                mysqlDB.Connect();
                //if (mysqlDB.oDB != null) {
                    mysqlDB.RunSqlNonQuery(sql);
                //}
            } catch (Exception ex) {
                Logger.LogError(6, "Problem creating database " + database + ": " + ex.StackTrace);
            } finally {
                if (mysqlDB != null)
                    mysqlDB.Disconnect();
            }
        }


        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Checks the given parameter for quotes and semicolons - removes if found</summary>
        public string SQLInjectionCheckParameter(bool doStrictTest, string paramText) {
            return SQLInjectionCheckParameter(doStrictTest, paramText, false);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public string SQLInjectionCheckParameter(bool doStrictTest, string paramText, bool ignoreQuotes) {
            return DatabaseHelper.SQL_INJECTION_CHECK_PARAMETER(doStrictTest, paramText, ignoreQuotes);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Returns the content of a DataTable object in a CSV format string. Column titles are included.</summary>
        public static string ConvertToCSV(DataTable aTable) {
            System.Text.StringBuilder tBuilder = new System.Text.StringBuilder();
            for (int i = 0; i < aTable.Columns.Count; i++) {
                DataColumn tCol = aTable.Columns[i];
                if (i > 0) {
                    tBuilder.Append(",");
                }
                tBuilder.Append("\"" + tCol.ColumnName + "\"");
            }
            tBuilder.Append("\r\n");
            bool tIsString = false;
            object tObj = null;
            string tFieldContent = "";
            for (int i = 0; i < aTable.Rows.Count; i++) {
                DataRow tRow = aTable.Rows[i];
                for (int j = 0; j < aTable.Columns.Count; j++) {
                    if (j != 0) {
                        tBuilder.Append(",");
                    }

                    tObj = tRow[j];
                    tIsString = tObj.GetType() == typeof(string);
                    tFieldContent = tObj.ToString();
                    if (tIsString || tFieldContent.Contains(",")) {
                        tBuilder.Append("\"");
                    }
                    tBuilder.Append(tObj.ToString());
                    if (tIsString || tFieldContent.Contains(",")) {
                        tBuilder.Append("\"");
                    }
                }
                tBuilder.Append("\r\n");
            }

            return tBuilder.ToString();
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>Returns the content of a strongly typed list that impliments the IList interface as a CSV format string. Coloumb titles are peramiter names.</summary>
        //Fix Me: need to add a srting list param for propertie names to ignore
        //Should check to make sure only "Flat" properties are used. I.e. no properties that are arrays.
        public static string ConvertToCSV(System.Collections.IList aList) {
            if (aList.Count <= 0) {
                return "";
            }

            System.Text.StringBuilder tBuilder = new System.Text.StringBuilder();

            bool tIsString = false;
            object tListObj = null;
            object tValObj = null;
            string tFieldContent = "";
            bool tFirstDone = false;
            System.Reflection.PropertyInfo[] tProps = null;

            for (int i = 0; i < aList.Count; i++) {
                //Get an object from the list
                tListObj = aList[i];

                //Get the col titles from the first item only
                if (i == 0) {
                    //Get the type of the object
                    Type tType = tListObj.GetType();

                    //Get all the public properties for that type
                    tProps = tType.GetProperties();

                    tFirstDone = false;
                    for (int k = 0; k < tProps.Length; k++) {
                        //Do not get titles from peramiters that cannot be read
                        if (tProps[k].CanRead) {
                            if (tFirstDone) {
                                tBuilder.Append(",");
                            }
                            tBuilder.Append("\"");
                            tBuilder.Append(tProps[k].Name);
                            tBuilder.Append("\"");
                            tFirstDone = true;
                        }
                    }
                    tBuilder.Append("\r\n");
                }

                tFirstDone = false;
                for (int j = 0; j < tProps.Length; j++) {
                    System.Reflection.PropertyInfo tProp = tProps[j];

                    if (tProp.CanRead) {
                        //Get the value for each public propertie
                        tValObj = tProp.GetValue(tListObj, null);
                        if (tFirstDone) {
                            tBuilder.Append(",");
                        }

                        tIsString = tValObj.GetType() == typeof(string);
                        tFieldContent = tValObj.ToString();
                        if (tIsString || tFieldContent.Contains(",")) {
                            tBuilder.Append("\"");
                        }
                        tBuilder.Append(tValObj.ToString());
                        if (tIsString || tFieldContent.Contains(",")) {
                            tBuilder.Append("\"");
                        }
                        tFirstDone = true;
                    }
                }
                tBuilder.Append("\r\n");
            }
            return tBuilder.ToString();
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public List<string> GetDatabaseNames() {
            List<string> dbNames = new List<string>();

            //if (this.oDB != null) {
                string tempDBN = "";
                IDataReader dbReader = null;
                try {
                    dbReader = RunSqlReader("SHOW DATABASES;");
                    while (dbReader.Read()) {
                        if (dbReader.IsDBNull(0) != true) {
                            tempDBN = dbReader.GetValue(0).ToString();
                        } else {
                            tempDBN = "";
                        }

                        dbNames.Add(tempDBN);
                    }
                    dbReader.Close();
                } catch (Exception ex) {
                    Logger.Log("Problem getting database names: " + ex.StackTrace);
                } finally {
                    if (dbReader != null && !dbReader.IsClosed) {
                        dbReader.Close();
                    }
                }
            //}

            return dbNames;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public void GrantPrivilegesToDatabase(string dbName) {
            DatabaseWrapper mysqlDB = null;
            try {
                // clone the DBConInfo and then change the db name to the generic mysql DB - this will therefore only work with MySQL
                DatabaseConnectionInfo tempDBConInfo = dbConInfo.Clone();
                tempDBConInfo.NAME = "mysql";

                mysqlDB = new DatabaseWrapper(tempDBConInfo);

                // use the default mysql port
//                mysqlDB = new DatabaseInformation(DBProvider, Server, "mysql", Username, Password, m_sConnectionPort);
                string sql =
                    "GRANT ALL PRIVILEGES ON " + dbName + ".* TO " + SecureStringWrapper.Decrypt(tempDBConInfo.USER)
                        + "@localhost IDENTIFIED BY '" + SecureStringWrapper.Decrypt(tempDBConInfo.PASSWORD) + @"';
                    GRANT ALL PRIVILEGES ON " + dbName + ".* TO " + SecureStringWrapper.Decrypt( tempDBConInfo.USER )
                        + "@\"%\" IDENTIFIED BY '" + SecureStringWrapper.Decrypt(tempDBConInfo.PASSWORD) + @"';
                    FLUSH PRIVILEGES;
                    ";
                mysqlDB.Connect();
                //if (mysqlDB.oDB != null) {
                    mysqlDB.RunSqlNonQuery(sql);
                //}
            } catch (Exception ex) {
                Logger.LogError(6, "Problem granting Privileges to database " + dbName + ": " + ex.StackTrace);
            } finally {
                if (mysqlDB != null)
                    mysqlDB.Disconnect();
            }
        }


        //------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Builds an alter table SQL query .... and indexes each column, if appropriate
        /// </summary>
        public string BuildAlterTableSQL(string[] colsToAddOrIndex, string[] colTypes, string tn) {
            StringBuilder alterTableSQL = new StringBuilder();

            for (int i = 0; i < colsToAddOrIndex.Length; i++) {
                string colName = colsToAddOrIndex[i];
                string colType = colTypes[i];

                if (alterTableSQL.Length > 0) {
                    alterTableSQL.Append(", ");
                }

                bool requireCol = false;
                bool requireIndex = false;
                if (ColumnExists(tn, colName) == false) {
                    requireCol = true;
                }
                if (CheckForColumnIndex(tn, colName) == false) {
                    requireIndex = true;
                }

                if (requireCol == true || requireIndex == true) {

                    if (requireCol) {
                        alterTableSQL.Append(colName + " " + colType);
                    }
                    if (requireCol && requireIndex) {
                        alterTableSQL.Append(", ");
                    }
                    if (requireIndex) {
                        alterTableSQL.Append("INDEX " + colName + "(" + colName + ")");
                    }
                }
            }

            if (alterTableSQL.Length > 0) {
                return "ALTER TABLE " + tn + " ADD ( " + alterTableSQL + ");";
            } else {
                return null;
            }
        }


        //------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     24-Aug-2015 - get the SSL parameters that verify that this connection is properly using SSL
        /// </summary>
        public bool IsSSLConnection(out string sslCipher, out string sslStartDate, out string sslEndDate, out string sslProtocol) {

            bool isSecure = false;

            // this group of neat little commands captures the cipher that is being used for the current connection and the TLS protocol!
            // This does give us some confidence that the connection is actually using SSL!
            // Get the Cipher, Transfer protocol and start and end date
            sslCipher = sslStartDate = sslEndDate = sslProtocol = "";

            string[] statusRow = ReadLine("show status where variable_name like 'Ssl_cipher';");
            sslCipher = (statusRow != null && statusRow.Length > 1) ? statusRow[1] : "";

            statusRow = ReadLine("show status where variable_name like 'Ssl_version';");
            sslProtocol = (statusRow != null && statusRow.Length > 1) ? statusRow[1] : "";

            statusRow = ReadLine("show status where variable_name like 'Ssl_server_not_before';");
            sslStartDate = (statusRow != null && statusRow.Length > 1) ? statusRow[1] : "";

            statusRow = ReadLine("show status where variable_name like 'Ssl_server_not_after';");
            sslEndDate = (statusRow != null && statusRow.Length > 1) ? statusRow[1] : "";

            // Now finally, lets see if we have a secure connection.  We could in the future also add the check that today is between
            // start and end date for the SSL certificate.  But it is too much for now ... and the connection would still be secure even if the
            // certificate was out of date.
            isSecure = (sslCipher != null && sslCipher != "" && sslProtocol != null && sslProtocol != "") ;

            return isSecure;
        }




        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// Holds the column names and description information for a table
        /// It maps the 'Describe Table' information
        /// </summary>
        public struct ColumnDescription {
            private string columnName;
            public string ColumnName {
                get { return columnName; }
                set { columnName = value; }
            }
            private string columnType;
            public string ColumnType {
                get { return columnType; }
                set { columnType = value; }
            }
            private bool isNull;
            public bool ISNull {
                get { return isNull; }
                set { isNull = value; }
            }
            private string keyName;
            public string KeyName {
                get { return keyName; }
                set { keyName = value; }
            }
            private string defaultValue;
            public string DefaultValue {
                get { return defaultValue; }
                set { defaultValue = value; }
            }
            private string extra;
            public string Extra {
                get { return extra; }
                set { extra = value; }
            }
            public ColumnDescription(string columnName, string columnType, bool isNull, string keyName, string defaultValue, string extra) {
                this.columnName = columnName;
                this.columnType = columnType;
                this.isNull = isNull;
                this.keyName = keyName;
                this.defaultValue = defaultValue;
                this.extra = extra;
            }

        }



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        //Added by Maz for Column and Indices information.
        //Contructor for storing Column names and corresponding indices information from a given table.
        public struct ColumnIndexDescriptor {
            private string columnName;
            public string ColumnName {
                get { return columnName; }
                set { columnName = value; }
            }

            private string keyName;
            public string KeyName {
                get { return keyName; }
                set { keyName = value; }
            }

            private string indexType;
            public string IndexType {
                get { return indexType; }
                set { indexType = value; }
            }
            public ColumnIndexDescriptor(string columnName, string keyName, string indexType) {
                this.columnName = columnName;
                this.keyName = keyName;
                this.indexType = indexType;
            }

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public struct ColumnDescriptor {
            private string columnName;

            public string ColumnName {
                get { return columnName; }
                set { columnName = value; }
            }

            private string colDesc;

            public string ColDesc {
                get { return colDesc; }
                set { colDesc = value; }
            }

            public ColumnDescriptor(string columnName, string colDesc) {
                this.columnName = columnName;
                this.colDesc = colDesc;

            }

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        private static readonly string TURN_ON_SQL_BIG_SELECTS = "SET SQL_BIG_SELECTS=1;";
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        private static readonly string TURN_OFF_SQL_BIG_SELECTS = "SET SQL_BIG_SELECTS=0;";




    }	// end of class
}
