using System;
using System.Data;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Security;
using MGL.Data.DataUtilities;


//-------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {

    //------------------------------------------------------------------------------------------------------------------------------------------------------------
    /// <summary>
    /// Summary description for Class1.
    ///     Adapted from a similar class developed by Manchester Geomatics Limited - link to Github...
    /// </summary>
    public class DatabaseWrapperBase : IDisposable {


        #region constructors / finalizer methods

        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        // Do not allow constructing the object directly
        protected DatabaseWrapperBase() {
            dbConnection = null;
            dbCommand = null;
            dbTransaction = null;
            dbConnectionString = null;
            dbCommandTimeout = 0;
            dbRetryConnect = 3;
            dbDisposed = false;
            dbConnected = false;
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        ~DatabaseWrapperBase() {
            this.Dispose(false);
        }

        #endregion

        #region private/protected members

        protected System.Data.IDbConnection dbConnection;
        protected System.Data.IDbCommand dbCommand;
        protected System.Data.IDbTransaction dbTransaction;
        protected SecureString dbConnectionString;
        protected int dbCommandTimeout;
        protected int dbRetryConnect;
        protected bool dbDisposed;
        protected bool dbConnected;

        protected string errors = null;
        #endregion

        #region Database connect / DisConnect / Transaction methods

        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool ValidateConnection() {

            if (dbConnected) {
                return true;
            } else {
                return Connect();
            }
        }



        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        public bool Connect() {
            // Check for valid connection string
            if (dbConnectionString == null || dbConnectionString.Length == 0) {
                throw (new Exception("Invalid database connection string"));
            }

            // Disconnect if already connected
            Disconnect();

            // Get ADONET connection object
            dbConnection = GetNewConnection();
            // Decrypt the SecureString to be consumed by the database ...
            dbConnection.ConnectionString = SecureStringWrapper.Decrypt(this.ConnectionString).ToString();

            // Implement connection retries
            for (int i = 0; i <= dbRetryConnect; i++) {
                try {
                    dbConnection.Open();

                    if (dbConnection.State == ConnectionState.Open) {
                        dbConnected = true;

                        break;
                    }
                } catch {
                    if (i == dbRetryConnect)
                        throw;

                    // Wait for 1 second and try again
                    Thread.Sleep(1000);
                }
            }

            // Get command object
            dbCommand = dbConnection.CreateCommand();
            dbCommand.CommandTimeout = dbCommandTimeout;

            return dbConnected;
        }


        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        public virtual void Disconnect() {
            // Disconnect can be called from Dispose and should guarantee no errors

            // stops a proper reconnection after a timeout
            //				if(!m_bConnected)
            //					return;

            try {
                if (dbTransaction != null) {
                    RollbackTransaction(false);
                }

                if (dbCommand != null) {
                    dbCommand.Dispose();
                    dbCommand = null;
                }

                if (dbConnection != null) {
                    try {
                        dbConnection.Close();
                    } catch {

                    } finally {
                        dbConnection.Dispose();
                        dbConnection = null;
                    }
                }

                dbConnected = false;
            } catch(Exception ex) {
                Logger.LogError(6, "Failed to disconnect: " + ex.StackTrace);
            }
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        public void BeginTransaction() {
            ValidateConnection();

            dbTransaction = dbConnection.BeginTransaction();
            dbCommand.Transaction = dbTransaction;

            //				return;
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        public void CommitTransaction() {
            if (dbTransaction == null) {
                throw (new Exception("BeginTransaction must be called before commit or rollback. No open transactions found"));
            }

            dbTransaction.Commit();
            dbTransaction.Dispose();
            dbTransaction = null;
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        public void RollbackTransaction() {
            RollbackTransaction(true);
        }

        //---------------------------------------------------------------------------------------------------------------------------------------------------------
        public void RollbackTransaction(bool bThrowError) {
            if (dbTransaction == null) {
                if (bThrowError)
                    throw (new Exception("BeginTransaction must be called before commit or rollback. No open transactions found"));
            }

            try {
                dbTransaction.Rollback();
            } catch {
                if (bThrowError)
                    throw;
            } finally {
                if (dbTransaction != null)
                    dbTransaction.Dispose();
                dbTransaction = null;
            }
        }

        #endregion

        #region Wraper methods for ADO.NET

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public IDataReader ExecuteReader(string sSQL) {
            return this.ExecuteReader(sSQL, CommandType.Text);
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public IDataReader ExecuteReader(string sSQL, CommandType oType) {
            try {
                ValidateConnection();

                dbCommand.CommandText = sSQL;
                dbCommand.CommandType = oType;

                return dbCommand.ExecuteReader();
            } catch (Exception ex) {		// dodgy connection, so reconnect

                this.errors = ex.ToString();

                ////!!!TEMP LOGGING!!!
                //CustomLog(sSQL, ex, "ExecuteReader.log");

                //                Disconnect();
                //                ValidateConnection();
                //                dbCommand.CommandText = sSQL;
                //                dbCommand.CommandType = oType;
                //                try {
                //                    return dbCommand.ExecuteReader();
                //                } catch (Exception ex2) {

                //                    this.errors = ex2.Message;
                //                    return null;
                //                }
            }
            return null;
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public DataSet GetDataSet(string sSQL) {
            DataSet oData = new DataSet();
            return GetDataSet(sSQL, CommandType.Text, oData);
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public DataSet GetDataSet(string sSQL, CommandType oType) {

            DataSet oData = new DataSet();
            return GetDataSet(sSQL, oType, oData);
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public DataSet GetDataSet(string sSQL, CommandType oType, DataSet oData) {

            try {

                ValidateConnection();

                dbCommand.CommandType = oType;
                dbCommand.CommandText = sSQL;

                IDataAdapter oAdpt = GetDataAdapter(sSQL);
                oAdpt.Fill(oData);


                return oData;
                //				} catch ( Exception ex ) {		// dodgy connection, so reconnect
            } catch (Exception ex) {		// dodgy connection, so reconnect
                this.errors = ex.ToString();
                //					m_bConnected = false;
                //Disconnect();
                //ValidateConnection();
                //dbCommand.CommandType = oType;
                //dbCommand.CommandText = sSQL;

                //oAdpt = GetDataAdapter(sSQL);
                //oAdpt.Fill(oData);

                //return oData;
            }
            return null;
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public object ExecuteScalar(string sSQL) {
            return ExecuteScalar(sSQL, CommandType.Text);
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public object ExecuteScalar(string sSQL, CommandType oType) {
            ValidateConnection();

            try {
                dbCommand.CommandText = sSQL;
                dbCommand.CommandType = oType;

                return dbCommand.ExecuteScalar();
                //				} catch ( Exception ex ) {		// dodgy connection, so reconnect
            } catch (Exception ex) {		// dodgy connection, so reconnect
                this.errors = ex.ToString();
                //					m_bConnected = false;
                //Disconnect();
                //ValidateConnection();
                //m_oCommand.CommandText = sSQL;
                //m_oCommand.CommandType = oType;
                //return m_oCommand.ExecuteScalar();
            }
            return null;
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public object ExecuteNonQuery(string sSQL) {
            return ExecuteNonQuery(sSQL, CommandType.Text);
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public object ExecuteNonQuery(string sSQL, CommandType oType) {
            ValidateConnection();

            try {
                dbCommand.CommandText = sSQL;
                dbCommand.CommandType = oType;

                return dbCommand.ExecuteNonQuery();
                //				} catch ( Exception ex ) {		// dodgy connection, so reconnect
            } catch (Exception ex) {		// dodgy connection, so reconnect
                this.errors = ex.ToString();
                //					m_bConnected = false;
                //Disconnect();
                //ValidateConnection();
                //m_oCommand.CommandText = sSQL;
                //m_oCommand.CommandType = oType;
                //return m_oCommand.ExecuteNonQuery();
            }
            return null;
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------
        /**
                EKS 5 October 2004
                Added so that the number modified can be returned
        */
        public int ExecuteSQL(string sSQL) {

            try {
                ValidateConnection();

                dbCommand.CommandText = sSQL;

                return dbCommand.ExecuteNonQuery();
            } catch (Exception ex) {		
                this.errors = ex.ToString();
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                //TEMP LOGGING TO CHECK THIS FUNCTION
                //ISN'T CAUSING A CONNECTION LEAK
                //CustomLog(sSQL, ex, "ExecuteUpdate.log");
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

//                Disconnect();
//                ValidateConnection();
//                m_oCommand.CommandText = sSQL;
//                return m_oCommand.ExecuteNonQuery();
            }
            return 0;
        }

        // ------------------------------------------------------------------------------------------------------------------------------------------------------
//        private void CustomLog(string sSql, Exception theEx, string logFileName) {
            //try
            //{
            //    string LogDir = "c:/temp/logs/";
            //    string LogFile = logFileName;
            //    string fullPath = LogDir + LogFile;

            //    SimpleIO sIo = new SimpleIO();
            //    if (SimpleIO.DirectoryExists(LogDir) == false)
            //    {
            //        sIo.CreateDirectory(LogDir);
            //    }

            //    if (!SimpleIO.FileExists(fullPath))
            //    {
            //        sIo.WriteToFile(fullPath, logFileName, false);
            //        sIo.WriteToFile(fullPath, "", true);
            //    }
            //    sIo.WriteToFile(fullPath, "", true);
            //    sIo.WriteToFile(fullPath, "-----" + DateTime.Now.ToString() + "-----", true);
            //    sIo.WriteToFile(fullPath, "The sql is as follows:", true);
            //    sIo.WriteToFile(fullPath, sSql, true);
            //    sIo.WriteToFile(fullPath, "The error message is as follows:", true);
            //    sIo.WriteToFile(fullPath, theEx.Message, true);
            //    sIo.WriteToFile(fullPath, "The StackTrace is as follows:", true);
            //    sIo.WriteToFile(fullPath, theEx.StackTrace, true);
            //    sIo.WriteToFile(fullPath, "--------------------------------------", true);
            //    sIo.WriteToFile(fullPath, "", true);
            //}
            //catch
            //{
            //}

//        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public IDataParameterCollection GetParameters() {

            try {
                ValidateConnection();

                return dbCommand.Parameters;
                //				} catch ( Exception ex ) {		// dodgy connection, so reconnect
            } catch (Exception ex ) {		// dodgy connection, so reconnect
                this.errors = ex.ToString();
                //					m_bConnected = false;
//                Disconnect();
//                ValidateConnection();
//                return m_oCommand.Parameters;
            }
            return null;
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public void AddParameter(IDataParameter oParam) {
            ValidateConnection();

            dbCommand.Parameters.Add(oParam);
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------
        public void ClearParameters() {
            if (dbCommand != null)
                dbCommand.Parameters.Clear();
        }

        #endregion
        
        #region Implementation of IDisposable
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        public void Dispose() {
            Dispose(true);
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        // Following is not IDisposable interface method. But added in this
        // region/section as it is more related to Dispose
        protected void Dispose(bool bDisposing) {
            if (dbDisposed == false) {
                // Dispose in this block, only managed resources
                if (bDisposing) {
                }

                // Free only un-managed resources here

            }

            dbDisposed = true;
        }

        #endregion

        #region properties (get/set methods)

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        public SecureString ConnectionString {
            get {
                return dbConnectionString;
            }
            set {
                dbConnectionString = value;
            }
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        public int CommandTimeout {
            get {
                return dbCommandTimeout;
            }
            set {
                dbCommandTimeout = value;
            }
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        public int ConnectionRetryCount {
            get {
                return dbRetryConnect;
            }
            set {
                dbRetryConnect = value;

                if (dbRetryConnect <= 0)
                    dbRetryConnect = 0;
            }
        }


        #endregion

        #region Utility functions

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        protected IDbConnection GetNewConnection() {
            IDbConnection oReturn = new MySqlConnection(); 
            return oReturn;

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        protected IDataAdapter GetDataAdapter(string sSQL) {
            IDataAdapter oReturn = new MySqlDataAdapter(sSQL, (MySqlConnection)dbConnection);

            ((MySqlDataAdapter)oReturn).SelectCommand = (MySqlCommand)dbCommand;

            return oReturn;
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        public IDataParameterCollection DeriveParameters(string sStoredProcedure) {
            return DeriveParameters(sStoredProcedure, CommandType.StoredProcedure);
        }
        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        public IDataParameterCollection DeriveParameters(string sSql, CommandType oType) {
            ValidateConnection();

            ClearParameters();

            dbCommand.CommandText = sSql;
            dbCommand.CommandType = oType;

            // Override here if needs be ...
           MySqlCommandBuilder.DeriveParameters((MySqlCommand)dbCommand);

            return dbCommand.Parameters;
        }

        #endregion



        //-----------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsConnected() {
            return this.dbConnected;
        }

    }
}
