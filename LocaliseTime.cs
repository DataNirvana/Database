using MGL.Data.DataUtilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

// This should be in DataNirvana.Security
//--------------------------------------------------------------------------------------------------------------------------------------------------------------------
namespace DataNirvana.Database {

    //----------------------------------------------------------------------------------------------------------------------------------------------------------------
    /// <summary>
    ///     25-Nov-2015 - A threadsafe way of localising the date and time, using the timezone information recorded in Security_Users_Location
    ///     This enables user specific timezones to be extracted, or a system level "front end" parameter or the default system settings
    ///     (the last one is normally UTC).
    /// </summary>
    public static class LocaliseTime {

        public static string LocationDBTN = "Security_Users_Location";

        //ConfigurationInfo ci;

        ////-------------------------------------------------------------------------------------------------------------------------------------------------------------
        //public LocaliseTime(ConfigurationInfo ci) {
        //    this.ci = ci;
        //}


        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     localisationOption can be 1=System default (normally UTC), 2=Frontend default, 3=User last login / access,
        ///     DEPRECATED - 4 - DEPRECATED=current login credentials
        ///     If 3 is not available, will default to 2; if 2 not available, will default to 1
        ///     4 is only really relevant in a couple of special cases, the PasswordResetRequest and the PasswordReset, as the user does
        ///     not actually login to complete these.  We now handle the collection of this information in these two pages, if it differs from
        ///     previous information.
        /// </summary>
        public static bool Localise(ConfigurationInfo ci, DateTime utcDT, int localisationOption, int userID, out DateTime localDT) {
            bool success = false;

            localDT = utcDT;
            int timezoneOffset = 0;

            try {

                if (DateTimeInformation.IsNullDate(utcDT) == false) {

                    if (localisationOption > 3 || localisationOption < 1 || (localisationOption == 3 && userID == 0)) {
                        Logger.LogError(6, "Unknown or invalid localisation option provided (" + localisationOption + " with userID " + userID + ") in LocaliseTime.Localise: " + Environment.StackTrace);
                    } else {

                        // try with 3
                        if (localisationOption == 3) {
                            success = GetTimezoneOffset(ci, 0, userID, out timezoneOffset);
                        }

                        // try with 2 or if 3 failed
                        if (localisationOption == 2 || (localisationOption == 3 && success == false)) {
                            success = GetTimezoneOffset(ci, 2, 0, out timezoneOffset);
                        }

                        // try with 1 or if 3 failed
                        if (localisationOption == 1 || success == false) {
                            success = GetTimezoneOffset(ci, 1, 0, out timezoneOffset);
                        }

                        if (success == true) {

                            // Timezones ahead of GMT / UTC actually are negative (a bit counterintuitively) so these will need to be added
                            if (timezoneOffset == 0) {
                                // Do nothing ...
                            } else if (timezoneOffset < 0) {
                                localDT = utcDT.Add(new TimeSpan(0, timezoneOffset * -1, 0));
                            } else {
                                localDT = utcDT.Subtract(new TimeSpan(0, timezoneOffset, 0));
                            }
                        }
                    }
                }

            } catch (Exception ex) {
                Logger.LogError( 5, "Problem localising the given date time ("+utcDT+") using localisation option "+localisationOption+" and userID "+userID+".  Check it out:" + ex.ToString());
            } finally {

            }

            return success;
        }
        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     localisationOption can be 1=System default (normally UTC), 2=Frontend default, 3=User last login / access,
        ///     DEPRECATED - 4 - DEPRECATED=current login credentials
        ///     If 3 is not available, will default to 2; if 2 not available, will default to 1
        ///     4 is only really relevant in a couple of special cases, the PasswordResetRequest and the PasswordReset, as the user does
        ///     not actually login to complete these.  We now handle the collection of this information in these two pages, if it differs from
        ///     previous information.
        /// </summary>
        public static bool Localise(ConfigurationInfo ci, DateTime utcDT, int localisationOption, int userID, out string prettyLocalDate) {
            bool success = false;

            prettyLocalDate = "";
            DateTime localDT = utcDT;

            success = Localise(ci, utcDT, localisationOption, userID, out localDT);

            // have to get the difference between the utcDT and the localDT so that we can generate the pretty version ...
            if (success == true) {
                TimeSpan t1 = utcDT.Subtract(localDT);
                int totalMinutes = (int) Math.Round(t1.TotalMinutes);
                prettyLocalDate = DateTimeInformation.PrettyDateTimeFormat(localDT, totalMinutes);
            }


            return success;
        }
        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Optimised localisation where the timezone offset is known and can be utilised for multiple entries
        ///     e.g. the search and export functions...
        ///     Note that this always returns true!
        ///     Returns the local date formulated in a pretty string ...
        /// </summary>
        public static string Localise(int timezoneOffset, DateTime utcDT) {

            DateTime localDT = Localise( utcDT, timezoneOffset );

            string prettyLocalDate = DateTimeInformation.PrettyDateTimeFormat(localDT, timezoneOffset);
            return prettyLocalDate;
        }
        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     The excel export code needs this version ...
        ///     Optimised localisation where the timezone offset is known and can be utilised for multiple entries
        ///     e.g. the search and export functions...
        ///     Note that this always returns true!
        ///     Returns the localised datetime object
        /// </summary>
        public static DateTime Localise(DateTime utcDT, int timezoneOffset) {

            DateTime localDate = utcDT;

            if (DateTimeInformation.IsNullDate(utcDT) == false) {

                // Timezones ahead of GMT / UTC actually are negative (a bit counterintuitively) so these will need to be added
                if (timezoneOffset == 0) {
                    // Do nothing ...
                } else if (timezoneOffset < 0) {
                    localDate = utcDT.Add(new TimeSpan(0, timezoneOffset * -1, 0));
                } else {
                    localDate = utcDT.Subtract(new TimeSpan(0, timezoneOffset, 0));
                }
            }

            return localDate;
        }

        //-------------------------------------------------------------------------------------------------------------------------------------------------------------
        /// <summary>
        ///     Actually gets the timezone offset from the database.  The locationID is the ID of the row in the Security_Users_Location table
        ///     and the UserID, is well, the user ID!!!  There are two special rows: 1 is the system default (normally UTC) and 2 is the application default (e.g. Pakistan)
        ///     It is rare that both attributes would be supplied together
        /// </summary>
        public static bool GetTimezoneOffset(ConfigurationInfo ci, int locationID, int userID, out int timezoneOffset) {
            bool success = false;
            timezoneOffset = 0;

            DatabaseWrapper dbInfo = null;

            try {

                if (locationID <= 0 && userID <= 0) {
                    Logger.LogError(5, "Problem getting the timezone offset - no location or user ID was specified!");
                } else {

                    dbInfo = new DatabaseWrapper(ci);

                    StringBuilder sql = new StringBuilder();
                    sql.Append("SELECT Timezone_Offset FROM " + LocationDBTN + " WHERE ");

                    if (locationID > 0 && userID > 0) {
                        // ID and User_ID
                        sql.Append("ID=" + locationID + " AND User_ID=" + userID);
                    } else if (userID > 0) {
                        // User_ID
                        sql.Append("User_ID=" + userID);
                    } else {
                        // ID
                        sql.Append("ID=" + locationID );
                    }

                    // ensure we only get 1 result returned which is the latest one
                    sql.Append(" ORDER BY Last_Login_Date DESC LIMIT 1;");

                    // now get the information from the query
                    List<int> tempResults = dbInfo.GetIntegerList(sql.ToString());
                    if (tempResults != null && tempResults.Count > 0) {
                        timezoneOffset = tempResults[0];
                        success = true;
                    } else {
                        // it is not necessarily an error here that the timezone has not been retrieved, but it is relatively unlikely; let's see how often it occurs
                        string IS_THIS_AN_ERROR;
                    }
                }
            } catch (Exception ex) {
                Logger.LogError( 5, "Problem getting the timezone offset for location "+locationID+" and userID "+userID+".  Check it out:" + ex.ToString());

            } finally {
                if (dbInfo != null) {
                    dbInfo.Disconnect();
                }
            }

            return success;
        }



    }
}
