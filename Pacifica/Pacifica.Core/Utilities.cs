using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using Jayrock.Json;
using Jayrock.Json.Conversion;

namespace Pacifica.Core
{
    public class Utilities
    {
        private static SHA1Managed _hashProvider;
        public static string GenerateSha1Hash(string filePath)
        {
            byte[] fileHash;
            var fi = new FileInfo(filePath);

            if (!fi.Exists)
                throw new FileNotFoundException("File not found in GenerateSha1Hash: " + filePath);

            if (_hashProvider == null)
            {
                _hashProvider = new SHA1Managed();
            }

            using (var sourceFile = new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                fileHash = _hashProvider.ComputeHash(sourceFile);
            }

            var hashString = ToHexString(fileHash);

            return hashString;
        }

        public static int GetDictionaryValue(Dictionary<string, string> dictionary, string keyName, int valueIfMissing)
        {
            var valueText = GetDictionaryValue(dictionary, keyName, valueIfMissing.ToString());

            if (int.TryParse(valueText, out int value))
                return value;

            return valueIfMissing;
        }

        public static string GetDictionaryValue(Dictionary<string, string> dictionary, string keyName, string valueIfMissing)
        {

            if (dictionary.TryGetValue(keyName, out string value))
            {
                return value ?? valueIfMissing;
            }

            return valueIfMissing;
        }

        public static List<FileInfoObject> GetFileListFromMetadataObject(List<Dictionary<string, object>> metadataObject)
        {
            var fileList = new List<FileInfoObject>();
            foreach (Dictionary<string, object> item in metadataObject)
            {
                if (item.TryGetValue("destinationTable", out object destTable))
                {
                    string t = (string)destTable;
                    if (t.ToLower() == "files")
                    {
                        fileList.Add(new FileInfoObject(
                            (string)item["absolutelocalpath"],
                            (string)item["subdir"],
                            (string)item["hashsum"]
                        ));
                    }
                }
            }

            return fileList;
        }

        public static DirectoryInfo GetTempDirectory()
        {
            DirectoryInfo di;
            if (!string.IsNullOrEmpty(Configuration.LocalTempDirectory))
            {
                di = new DirectoryInfo(Configuration.LocalTempDirectory);
            }
            else
            {
                di = new DirectoryInfo(Path.GetTempPath());
            }
            return di;
        }

        public static string ToHexString(byte[] buffer)
        {
            return BitConverter.ToString(buffer).Replace("-", string.Empty).ToLower();
        }

        public static Dictionary<string, object> JsonToObject(string jsonString)
        {
            var jso = (JsonObject)JsonConvert.Import(jsonString);
            return JsonObjectToDictionary(jso);
        }

        public static string ObjectToJson(IList mdObject)
        {
            var jso = new JsonArray(mdObject);
            return jso.ToString();
        }

        public static Dictionary<string, object> JsonObjectToDictionary(JsonObject jso)
        {
            var d = new Dictionary<string, object>();

            if (jso == null)
            {
                Console.WriteLine("Skipping null item in JsonObjectToDictionary");
                return d;
            }

            foreach (string key in jso.Names)
            {
                if (jso[key] == null)
                {
                    jso[key] = string.Empty;
                }

                var value = jso[key];
                if (value.GetType().Name == "JsonObject")
                {
                    var tmpJso = value as JsonObject;
                    d.Add(key, JsonObjectToDictionary(tmpJso));  //Recurse!
                }
                else if (value.GetType().Name == "JsonArray")
                {
                    try
                    {
                        var tmpJsa = value as JsonArray;
                        switch (key)
                        {
                            case "users":
                                // EUS User IDs are always integers
                                d.Add(key, JsonArrayToIntList(tmpJsa));
                                break;

                            case "proposals":
                                // EUS Proposals are usually integers, but not always
                                // Thus, store as strings
                                d.Add(key, JsonArrayToStringList(tmpJsa));
                                break;

                            default:
                                if (tmpJsa == null || tmpJsa.Count == 0)
                                {
                                    d.Add(key, new List<Dictionary<string, object>>());
                                }
                                else
                                {
                                    var nextValue = tmpJsa.GetValue(0);
                                    if (nextValue == null)
                                    {
                                        d.Add(key, new List<Dictionary<string, object>>());
                                    }
                                    else
                                    {
                                        var typeName = nextValue.GetType().Name;

                                        if (typeName == "String" || typeName == "JsonNumber")
                                            d.Add(key, JsonArrayToStringList(tmpJsa));
                                        else
                                            d.Add(key, JsonArrayToDictionaryList(tmpJsa));
                                    }
                                }
                                break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Error in parsing a JsonArray in JsonObjectToDictionary:" + ex.Message);
                    }

                }
                else
                {
                    d.Add(key, value);
                }
            }

            return d;
        }

        public static List<string> JsonArrayToStringList(JsonArray jsa)
        {
            var lstItems = new List<string>();

            while (jsa.Length > 0)
            {
                var value = jsa.Pop();
                var typeName = value.GetType().Name;
                if (typeName == "JsonNumber" || typeName == "String")
                {
                    lstItems.Add(value.ToString());
                }
                else
                {
                    throw new InvalidCastException("JsonArrayToStringList cannot process an item of type " + typeName);
                }
            }

            return lstItems;
        }

        public static List<int> JsonArrayToIntList(JsonArray jsa)
        {
            var lstStrings = JsonArrayToStringList(jsa);
            var lstInts = new List<int>();

            foreach (var item in lstStrings)
            {
                if (int.TryParse(item, out int value))
                    lstInts.Add(value);
                else
                    throw new InvalidCastException("JsonArrayToIntList cannot convert item '" + value + "' to an integer");
            }

            return lstInts;
        }

        public static List<Dictionary<string, object>> JsonArrayToDictionaryList(JsonArray jsa)
        {
            var lstItems = new List<Dictionary<string, object>>();
            while (jsa.Length > 0)
            {
                var value = jsa.Pop();
                if (value.GetType().Name == "JsonNumber")
                {
                    var dctValue = new Dictionary<string, object>();
                    dctValue.Add(value.ToString(), string.Empty);
                    lstItems.Add(dctValue);
                }
                else if (value.GetType().Name == "String")
                {
                    var dctValue = new Dictionary<string, object>();
                    dctValue.Add(value.ToString(), string.Empty);
                    lstItems.Add(dctValue);
                }
                else if (value.GetType().Name == "JsonObject")
                {
                    var jso = (JsonObject)value;
                    lstItems.Add(JsonObjectToDictionary(jso));
                }
                else
                {
                    Console.WriteLine("Unsupported JsonArrayList type: " + value.GetType().Name);
                }
            }
            return lstItems;
        }


        public static string GetMetadataFilenameForJob(string jobNumber)
        {
            if (string.IsNullOrWhiteSpace(jobNumber))
                return "MyEMSL_metadata_CaptureJob_000000.txt";
            else
                return "MyEMSL_metadata_CaptureJob_" + jobNumber + ".txt";
        }

        public static string GetUserName(bool cleanDomain = false)
        {
            var userIdent = WindowsIdentity.GetCurrent();
            var userName = userIdent.Name;

            if (cleanDomain)
            {
                userName = userName.Substring(userName.IndexOf('\\') + 1);
            }

            return userName;
        }

        /// <summary>
        /// Callback used to validate the certificate in an SSL conversation 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="cert"></param>
        /// <param name="chain"></param>
        /// <param name="policyErrors"></param>
        /// <returns>True if the server is trusted</returns>
        public static bool ValidateRemoteCertificate(object sender, X509Certificate cert, X509Chain chain, SslPolicyErrors policyErrors)
        {
            var lstTrustedDomains = new List<string>
            {
                "my.emsl.pnl.gov",
                "emsl.pnl.gov",
                "pnl.gov"
            };

            var reExtractCN = new System.Text.RegularExpressions.Regex("CN=([^ ,]+),");
            var reMatch = reExtractCN.Match(cert.Subject);
            string domainToValidate;

            if (reMatch.Success)
                domainToValidate = reMatch.Groups[1].ToString();
            else
                domainToValidate = cert.Subject;

            // Console.WriteLine("Checking " + domainToValidate + " against trusted domains");

            foreach (var domainName in lstTrustedDomains)
            {
                if (domainToValidate.IndexOf(domainName, StringComparison.CurrentCultureIgnoreCase) >= 0)
                    return true;
            }

            return false;
        }
    }
}
