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

            var lstCredentials = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            var sShareFolderPath = @"\\15T_FTICR.bionet\ProteomicsData";

            // Make sure sShareFolderPath does not end in a back slash
            if (sShareFolderPath.EndsWith(@"\"))
                sShareFolderPath = sShareFolderPath.Substring(0, sShareFolderPath.Length - 1);

            lstCredentials.Add("ftms", "PasswordHere");
            lstCredentials.Add("lcmsoperator", "PasswordHere");

            try
            {
                using (var enumCurrent = lstCredentials.GetEnumerator())
                {

                    while (enumCurrent.MoveNext())
                    {
                        var accessCredentials = new System.Net.NetworkCredential(enumCurrent.Current.Key,
                                                                                 enumCurrent.Current.Value, "");

                        Console.WriteLine(@"Credentials created for " + enumCurrent.Current.Key);

                        var cnBionet = new NetworkConnection(sShareFolderPath, accessCredentials);

                        Console.WriteLine(@"Connected to share");

                        var diDirectory = new System.IO.DirectoryInfo(sShareFolderPath);

                        Console.WriteLine(@"Instantiated DirectoryInfo object: " + diDirectory.FullName);

                        var iIterations = 0;
                        foreach (var fiFile in diDirectory.GetFiles())
                        {
                            Console.WriteLine(@"File: " + fiFile.Name + @" size " + fiFile.Length + @" bytes");
                            ++iIterations;

                            if (iIterations > 20)
                                break;
                        }

                        Console.WriteLine(@"Files Done");

                        iIterations = 0;
                        foreach (var diFolder in diDirectory.GetDirectories())
                        {
                            Console.WriteLine(@"Folder: " + diFolder.Name + @" modified " +
                                              diFolder.LastWriteTime.ToString(CultureInfo.InvariantCulture));
                            ++iIterations;

                            if (iIterations > 20)
                                break;
                        }

                        Console.WriteLine(@"Folders Done");

                        // Disconnect network connection
                        cnBionet.Dispose();
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