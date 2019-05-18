using System;
using System.Collections.Generic;
using System.Globalization;

namespace CaptureTaskManager
{
    class clsCodeTest
    {

        public void TestConnection()
        {
            Console.WriteLine(@"Code test mode");

            var credentials = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var sharePath = @"\\15T_FTICR.bionet\ProteomicsData";

            // Make sure sharePath does not end in a back slash
            if (sharePath.EndsWith(@"\"))
                sharePath = sharePath.Substring(0, sharePath.Length - 1);

            credentials.Add("ftms", "PasswordHere");
            credentials.Add("lcmsoperator", "PasswordHere");

            try
            {
                using (var enumCurrent = credentials.GetEnumerator())
                {

                    while (enumCurrent.MoveNext())
                    {
                        var accessCredentials = new System.Net.NetworkCredential(enumCurrent.Current.Key,
                                                                                 enumCurrent.Current.Value, "");

                        Console.WriteLine(@"Credentials created for " + enumCurrent.Current.Key);

                        var bionetConnection = new NetworkConnection(sharePath, accessCredentials);

                        Console.WriteLine(@"Connected to share");

                        var shareDirectory = new System.IO.DirectoryInfo(sharePath);

                        Console.WriteLine(@"Instantiated DirectoryInfo object: " + shareDirectory.FullName);

                        var iteration = 0;
                        foreach (var remoteFile in shareDirectory.GetFiles())
                        {
                            Console.WriteLine(@"File: " + remoteFile.Name + @" size " + remoteFile.Length + @" bytes");
                            ++iteration;

                            if (iteration > 20)
                                break;
                        }

                        Console.WriteLine(@"Files Done");

                        iteration = 0;
                        foreach (var subdirectory in shareDirectory.GetDirectories())
                        {
                            Console.WriteLine(@"Folder: " + subdirectory.Name + @" modified " +
                                              subdirectory.LastWriteTime.ToString(CultureInfo.InvariantCulture));
                            ++iteration;

                            if (iteration > 20)
                                break;
                        }

                        Console.WriteLine(@"Folders Done");

                        // Disconnect network connection
                        bionetConnection.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(@"Exception: " + ex.Message);
            }
        }

    }
}