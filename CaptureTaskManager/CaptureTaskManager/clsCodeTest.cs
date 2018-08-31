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

            var sharePath = @"\\15T_FTICR.bionet\ProteomicsData";

            // Make sure sharePath does not end in a back slash
            if (sharePath.EndsWith(@"\"))
                sharePath = sharePath.Substring(0, sharePath.Length - 1);

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

                        var cnBionet = new NetworkConnection(sharePath, accessCredentials);

                        Console.WriteLine(@"Connected to share");

                        var diDirectory = new System.IO.DirectoryInfo(sharePath);

                        Console.WriteLine(@"Instantiated DirectoryInfo object: " + diDirectory.FullName);

                        var iteration = 0;
                        foreach (var fiFile in diDirectory.GetFiles())
                        {
                            Console.WriteLine(@"File: " + fiFile.Name + @" size " + fiFile.Length + @" bytes");
                            ++iteration;

                            if (iteration > 20)
                                break;
                        }

                        Console.WriteLine(@"Files Done");

                        iteration = 0;
                        foreach (var diFolder in diDirectory.GetDirectories())
                        {
                            Console.WriteLine(@"Folder: " + diFolder.Name + @" modified " +
                                              diFolder.LastWriteTime.ToString(CultureInfo.InvariantCulture));
                            ++iteration;

                            if (iteration > 20)
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