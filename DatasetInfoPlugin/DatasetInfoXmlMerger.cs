using PNNLOmics.Utilities;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace DatasetInfoPlugin
{
    internal class DatasetInfoXmlMerger
    {
        // Ignore Spelling: yyyy-MM-dd hh:mm:ss tt

        private const int DATASET_GAP_THRESHOLD_HOURS = 24;

        private struct AcquisitionInfo
        {
            public int ScanCount;
            public int ScanCountMS;
            public int ScanCountMSn;
            public double ElutionTimeMax;
            public double AcqTimeMinutes;
            public DateTime StartTime;
            public DateTime EndTime;
            public long FileSizeBytes;
            public int ProfileScanCountMS1;
            public int ProfileScanCountMS2;
            public int CentroidScanCountMS1;
            public int CentroidScanCountMS2;
            public int CentroidMS1ScansClassifiedAsProfile;
            public int CentroidMS2ScansClassifiedAsProfile;

            public void Clear()
            {
                ScanCount = 0;
                ScanCountMS = 0;
                ScanCountMSn = 0;
                ElutionTimeMax = 0;
                AcqTimeMinutes = 0;
                StartTime = DateTime.MinValue;
                EndTime = DateTime.MinValue;
                FileSizeBytes = 0;
                ProfileScanCountMS1 = 0;
                ProfileScanCountMS2 = 0;
                CentroidScanCountMS1 = 0;
                CentroidScanCountMS2 = 0;
                CentroidMS1ScansClassifiedAsProfile = 0;
                CentroidMS2ScansClassifiedAsProfile = 0;
            }
        }

        private struct TicInfo
        {
            public double TIC_Max_MS;
            public double TIC_Max_MSn;
            public double BPI_Max_MS;
            public double BPI_Max_MSn;
            public double TIC_Median_MS;
            public double TIC_Median_MSn;
            public double BPI_Median_MS;
            public double BPI_Median_MSn;

            public void Clear()
            {
                TIC_Max_MS = 0;
                TIC_Max_MSn = 0;
                BPI_Max_MS = 0;
                BPI_Max_MSn = 0;
                TIC_Median_MS = 0;
                TIC_Median_MSn = 0;
                BPI_Median_MS = 0;
                BPI_Median_MSn = 0;
            }
        }

        private struct DatasetAcqTimeInfo
        {
            public string Dataset;
            public DateTime StartTime;
            public DateTime EndTime;
        }

        public struct InstrumentFileInfo
        {
            /// <summary>
            /// Filename
            /// </summary>
            public string Filename;

            /// <summary>
            /// Hash of the file contents
            /// </summary>
            public string Hash;

            /// <summary>
            /// Hash type (typically SHA-1)
            /// </summary>
            public string HashType;

            /// <summary>
            /// File size, in bytes
            /// </summary>
            public long FileSize;
        }

        /// <summary>
        /// List of warnings about datasets that start more than 120 minutes after the previous dataset
        /// </summary>
        public List<string> AcqTimeWarnings { get; }

        /// <summary>
        /// List of instruments files that MSFileInfoScanner computed a SHA-1 hash of the file contents
        /// </summary>
        public List<InstrumentFileInfo> InstrumentFiles { get; }

        /// <summary>
        /// The keys in this dictionary are KeyValuePairs of [ScanType,ScanFilterText] while the values are the scan count
        /// </summary>
        public Dictionary<KeyValuePair<string, string>, int> ScanTypes { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public DatasetInfoXmlMerger()
        {
            AcqTimeWarnings = new List<string>();
            InstrumentFiles = new List<InstrumentFileInfo>();
            ScanTypes = new Dictionary<KeyValuePair<string, string>, int>();
        }

        /// <summary>
        /// Merge the dataset info defined in cachedDatasetInfoXml
        /// </summary>
        /// <param name="datasetName">Dataset Name override</param>
        /// <param name="cachedDatasetInfoXml">List of cached DatasetInfo XML</param>
        /// <returns>Merged DatasetInfo XML</returns>
        public string CombineDatasetInfoXML(string datasetName, List<string> cachedDatasetInfoXml)
        {
            AcqTimeWarnings.Clear();

            if (cachedDatasetInfoXml.Count == 1)
            {
                return cachedDatasetInfoXml.First();
            }

            // Keys in this dictionary are KeyValuePairs of [ScanType,ScanFilterText] while the values are the scan count
            ScanTypes.Clear();

            var acqInfo = new AcquisitionInfo();
            acqInfo.Clear();

            var ticInfo = new TicInfo();
            ticInfo.Clear();

            // Keys in this dictionary are dataset names; values track the StartTime and EndTime for the dataset
            var datasetAcqTimes = new Dictionary<string, DatasetAcqTimeInfo>();

            // Parse each block of cached XML
            foreach (var cachedInfoXml in cachedDatasetInfoXml)
            {
                var currentDatasetName = ParseDatasetInfoXml(cachedInfoXml, ref acqInfo, ref ticInfo, datasetAcqTimes);

                if (string.IsNullOrWhiteSpace(datasetName) && !string.IsNullOrWhiteSpace(currentDatasetName))
                {
                    datasetName = currentDatasetName;
                }
            }

            // Make sure none of the datasets has a start time more than 120 minutes after the previous dataset's end time
            // If it does, the operator likely lumped together unrelated datasets, and therefore the overall dataset stats will be wrong
            var sortedDsAcqTimes = (from item in datasetAcqTimes orderby item.Value.StartTime select item.Value).ToList();

            for (var i = 1; i < sortedDsAcqTimes.Count; i++)
            {
                var spanHours = sortedDsAcqTimes[i].StartTime.Subtract(sortedDsAcqTimes[i - 1].EndTime).TotalHours;

                if (spanHours > DATASET_GAP_THRESHOLD_HOURS)
                {
                    var warningMsg = string.Format("Dataset {0} starts {1:F1} hours after {2}; the datasets appear unrelated",
                                                   sortedDsAcqTimes[i].Dataset, spanHours, sortedDsAcqTimes[i - 1].Dataset);
                    AcqTimeWarnings.Add(warningMsg);
                }
            }

            // Create the combined XML
            var combinedXml = CreateDatasetInfoXML(datasetName, ScanTypes, acqInfo, ticInfo);

            // Return the XML as text
            return combinedXml;
        }

        /// <summary>
        /// Parse the XML to populate scanTypes, acqInfo, and ticInfo
        /// </summary>
        /// <param name="cachedInfoXml">XML to parse</param>
        /// <param name="acqInfo">Merged acquisition info</param>
        /// <param name="ticInfo">Merged TIC info</param>
        /// <param name="datasetAcqTimes">Tracks the start and end time for each dataset</param>
        /// <returns>Dataset name</returns>
        private string ParseDatasetInfoXml(
            string cachedInfoXml,
            ref AcquisitionInfo acqInfo,
            ref TicInfo ticInfo,
            IDictionary<string, DatasetAcqTimeInfo> datasetAcqTimes)

        {
            var xmlDoc = new XmlDocument();

            using (var reader = new StringReader(cachedInfoXml))
            {
                xmlDoc.Load(reader);
            }

            var datasetName = GetXmlValue(xmlDoc, "DatasetInfo/Dataset", string.Empty);

            var scanTypeNodes = xmlDoc.SelectNodes("DatasetInfo/ScanTypes/ScanType");

            if (scanTypeNodes != null)
            {
                foreach (XmlNode node in scanTypeNodes)
                {
                    KeyValuePair<string, string> scanTypeKey;
                    int scanTypeScanCount;
                    if (node.Attributes != null)
                    {
                        var scanFilterText = node.Attributes["ScanFilterText"].Value;
                        scanTypeKey = new KeyValuePair<string, string>(node.InnerText, scanFilterText);

                        var scanCountText = node.Attributes["ScanCount"].InnerText;
                        int.TryParse(scanCountText, out scanTypeScanCount);
                    }
                    else
                    {
                        scanTypeKey = new KeyValuePair<string, string>(node.InnerText, string.Empty);
                        scanTypeScanCount = 0;
                    }

                    if (!ScanTypes.TryGetValue(scanTypeKey, out var scanCountTotal))
                    {
                        ScanTypes.Add(scanTypeKey, scanTypeScanCount);
                    }
                    else
                    {
                        ScanTypes[scanTypeKey] = scanCountTotal + scanTypeScanCount;
                    }
                }
            }

            acqInfo.ScanCount += GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/ScanCount", 0);

            var oldScanCountMSOverall = acqInfo.ScanCountMS;
            var oldScanCountMSnOverall = acqInfo.ScanCountMSn;

            var currentEntryScanCountMS = GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/ScanCountMS", 0);
            var currentEntryScanCountMSn = GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/ScanCountMSn", 0);
            acqInfo.ScanCountMS += currentEntryScanCountMS;
            acqInfo.ScanCountMSn += currentEntryScanCountMSn;

            var elutionTimeMax = GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/Elution_Time_Max", 0.0);
            if (elutionTimeMax > acqInfo.ElutionTimeMax)
            {
                acqInfo.ElutionTimeMax = elutionTimeMax;
            }

            acqInfo.AcqTimeMinutes += GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/AcqTimeMinutes", 0.0);

            var startTimeText = GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/StartTime", string.Empty);
            var endTimeText = GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/EndTime", string.Empty);

            var startTimeValid = false;

            if (DateTime.TryParse(startTimeText, out var startTime))
            {
                startTimeValid = true;
                if (acqInfo.StartTime == DateTime.MinValue)
                {
                    acqInfo.StartTime = startTime;
                }
                else if (startTime < acqInfo.StartTime)
                {
                    acqInfo.StartTime = startTime;
                }
            }

            if (DateTime.TryParse(endTimeText, out var endTime))
            {
                if (acqInfo.EndTime == DateTime.MinValue)
                {
                    acqInfo.EndTime = endTime;
                }
                else if (endTime > acqInfo.EndTime)
                {
                    acqInfo.EndTime = endTime;
                }

                if (startTimeValid && !datasetAcqTimes.ContainsKey(datasetName))
                {
                    var acqTimeInfo = new DatasetAcqTimeInfo
                    {
                        Dataset = datasetName,
                        StartTime = startTime,
                        EndTime = endTime
                    };

                    datasetAcqTimes.Add(datasetName, acqTimeInfo);
                }
            }

            acqInfo.FileSizeBytes += GetXmlValueLong(xmlDoc, "DatasetInfo/AcquisitionInfo/FileSizeBytes", 0);

            acqInfo.ProfileScanCountMS1 += GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/ProfileScanCountMS1", 0);
            acqInfo.ProfileScanCountMS2 += GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/ProfileScanCountMS2", 0);

            acqInfo.CentroidScanCountMS1 += GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/CentroidScanCountMS1", 0);
            acqInfo.CentroidScanCountMS2 += GetXmlValue(xmlDoc, "DatasetInfo/AcquisitionInfo/CentroidScanCountMS2", 0);

            acqInfo.CentroidMS1ScansClassifiedAsProfile += GetXmlValue(xmlDoc,
                                                                       "DatasetInfo/AcquisitionInfo/CentroidMS1ScansClassifiedAsProfile", 0);
            acqInfo.CentroidMS2ScansClassifiedAsProfile += GetXmlValue(xmlDoc,
                                                                       "DatasetInfo/AcquisitionInfo/CentroidMS2ScansClassifiedAsProfile", 0);

            var instrumentFileNodes = xmlDoc.SelectNodes("DatasetInfo/AcquisitionInfo/InstrumentFiles/InstrumentFile");

            if (instrumentFileNodes != null)
            {
                foreach (XmlNode node in instrumentFileNodes)
                {
                    var fileInfo = new InstrumentFileInfo
                    {
                        Filename = node.InnerText
                    };

                    if (node.Attributes != null)
                    {
                        fileInfo.Hash = node.Attributes["Hash"].Value;
                        fileInfo.HashType = node.Attributes["HashType"].Value;
                        var fileSizeText = node.Attributes["Size"].Value;

                        long.TryParse(fileSizeText, out var fileSize);
                        fileInfo.FileSize = fileSize;
                    }
                    else
                    {
                        fileInfo.Hash = string.Empty;
                        fileInfo.HashType = string.Empty;
                        fileInfo.FileSize = 0;
                    }

                    InstrumentFiles.Add(fileInfo);
                }
            }

            ticInfo.TIC_Max_MS = Math.Max(ticInfo.TIC_Max_MS, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/TIC_Max_MS", 0.0));
            ticInfo.TIC_Max_MSn = Math.Max(ticInfo.TIC_Max_MSn, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/TIC_Max_MSn", 0.0));
            ticInfo.BPI_Max_MS = Math.Max(ticInfo.BPI_Max_MS, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/BPI_Max_MS", 0.0));
            ticInfo.BPI_Max_MSn = Math.Max(ticInfo.BPI_Max_MSn, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/BPI_Max_MSn", 0.0));

            ticInfo.TIC_Median_MS = UpdateAverage(
                oldScanCountMSOverall, ticInfo.TIC_Median_MS,
                currentEntryScanCountMS, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/TIC_Median_MS", 0.0));

            ticInfo.TIC_Median_MSn = UpdateAverage(
                oldScanCountMSnOverall, ticInfo.TIC_Median_MSn,
                currentEntryScanCountMSn, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/TIC_Median_MSn", 0.0));

            ticInfo.BPI_Median_MS = UpdateAverage(
                oldScanCountMSOverall, ticInfo.BPI_Median_MS,
                currentEntryScanCountMS, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/BPI_Median_MS", 0.0));

            ticInfo.BPI_Median_MSn = UpdateAverage(
                oldScanCountMSnOverall, ticInfo.BPI_Median_MSn,
                currentEntryScanCountMSn, GetXmlValue(xmlDoc, "DatasetInfo/TICInfo/BPI_Median_MSn", 0.0));

            return datasetName;
        }

        private string CreateDatasetInfoXML(
            string datasetName,
            AcquisitionInfo acqInfo,
            TicInfo ticInfo)
        {
            var xmlSettings = new XmlWriterSettings
            {
                CheckCharacters = true,
                Indent = true,
                IndentChars = "  ",
                Encoding = Encoding.UTF8,
            };

            var memoryStream = new MemoryStream();
            using var xWriter = XmlWriter.Create(memoryStream, xmlSettings);

            xWriter.WriteStartDocument(true);

            // Write the beginning of the "Root" element.
            xWriter.WriteStartElement("DatasetInfo");

            xWriter.WriteElementString("Dataset", datasetName);

            xWriter.WriteStartElement("ScanTypes");

            foreach (var scanType in ScanTypes)
            {
                var scanTypeName = scanType.Key.Key;
                var scanFilterText = scanType.Key.Value;
                var scanTypeCount = scanType.Value;

                xWriter.WriteStartElement("ScanType");
                xWriter.WriteAttributeString("ScanCount", scanTypeCount.ToString());
                xWriter.WriteAttributeString("ScanFilterText", scanFilterText);
                xWriter.WriteString(scanTypeName);
                xWriter.WriteEndElement(); // ScanType EndElement
            }

            xWriter.WriteEndElement(); // ScanTypes EndElement

            xWriter.WriteStartElement("AcquisitionInfo");

            xWriter.WriteElementString("ScanCount", acqInfo.ScanCount.ToString());

            xWriter.WriteElementString("ScanCountMS", acqInfo.ScanCountMS.ToString());
            xWriter.WriteElementString("ScanCountMSn", acqInfo.ScanCountMSn.ToString());
            xWriter.WriteElementString("Elution_Time_Max", acqInfo.ElutionTimeMax.ToString("0.00"));

            xWriter.WriteElementString("AcqTimeMinutes", acqInfo.AcqTimeMinutes.ToString("0.00"));
            xWriter.WriteElementString("StartTime", acqInfo.StartTime.ToString("yyyy-MM-dd hh:mm:ss tt"));
            xWriter.WriteElementString("EndTime", acqInfo.EndTime.ToString("yyyy-MM-dd hh:mm:ss tt"));

            xWriter.WriteElementString("FileSizeBytes", acqInfo.FileSizeBytes.ToString());

            if (acqInfo.ProfileScanCountMS1 > 0 || acqInfo.ProfileScanCountMS2 > 0 ||
                acqInfo.CentroidScanCountMS1 > 0 || acqInfo.CentroidScanCountMS2 > 0 ||
                acqInfo.CentroidMS1ScansClassifiedAsProfile > 0 || acqInfo.CentroidMS2ScansClassifiedAsProfile > 0)
            {
                xWriter.WriteElementString("ProfileScanCountMS1", acqInfo.ProfileScanCountMS1.ToString());
                xWriter.WriteElementString("ProfileScanCountMS2", acqInfo.ProfileScanCountMS2.ToString());

                xWriter.WriteElementString("CentroidScanCountMS1", acqInfo.CentroidScanCountMS1.ToString());
                xWriter.WriteElementString("CentroidScanCountMS2", acqInfo.CentroidScanCountMS2.ToString());

                if (acqInfo.CentroidMS1ScansClassifiedAsProfile > 0 || acqInfo.CentroidMS2ScansClassifiedAsProfile > 0)
                {
                    xWriter.WriteElementString("CentroidMS1ScansClassifiedAsProfile",
                        acqInfo.CentroidMS1ScansClassifiedAsProfile.ToString());
                    xWriter.WriteElementString("CentroidMS2ScansClassifiedAsProfile",
                        acqInfo.CentroidMS2ScansClassifiedAsProfile.ToString());
                }
            }

            if (InstrumentFiles.Count > 0)
            {
                xWriter.WriteStartElement("InstrumentFiles");

                foreach (var instrumentFile in InstrumentFiles)
                {
                    xWriter.WriteStartElement("InstrumentFile");
                    xWriter.WriteAttributeString("Hash", instrumentFile.Hash);
                    xWriter.WriteAttributeString("HashType", instrumentFile.HashType);
                    xWriter.WriteAttributeString("Size", instrumentFile.FileSize.ToString());
                    xWriter.WriteString(instrumentFile.Filename);
                    xWriter.WriteEndElement(); // InstrumentFile EndElement
                }

                xWriter.WriteEndElement(); // InstrumentFiles EndElement
            }

            xWriter.WriteEndElement(); // AcquisitionInfo EndElement

            xWriter.WriteStartElement("TICInfo");
            xWriter.WriteElementString("TIC_Max_MS", StringUtilities.ValueToString(ticInfo.TIC_Max_MS, 5));
            xWriter.WriteElementString("TIC_Max_MSn", StringUtilities.ValueToString(ticInfo.TIC_Max_MSn, 5));
            xWriter.WriteElementString("BPI_Max_MS", StringUtilities.ValueToString(ticInfo.BPI_Max_MS, 5));
            xWriter.WriteElementString("BPI_Max_MSn", StringUtilities.ValueToString(ticInfo.BPI_Max_MSn, 5));
            xWriter.WriteElementString("TIC_Median_MS", StringUtilities.ValueToString(ticInfo.TIC_Median_MS, 5));
            xWriter.WriteElementString("TIC_Median_MSn", StringUtilities.ValueToString(ticInfo.TIC_Median_MSn, 5));
            xWriter.WriteElementString("BPI_Median_MS", StringUtilities.ValueToString(ticInfo.BPI_Median_MS, 5));
            xWriter.WriteElementString("BPI_Median_MSn", StringUtilities.ValueToString(ticInfo.BPI_Median_MSn, 5));

            xWriter.WriteEndElement(); // TICInfo EndElement

            xWriter.WriteEndElement(); // End the "Root" element (DatasetInfo)

            // Close out the XML document (but do not close XWriter yet)
            xWriter.WriteEndDocument();
            xWriter.Flush();

            // Now use a StreamReader to copy the XML text to a string variable
            memoryStream.Seek(0, SeekOrigin.Begin);

            var memoryStreamReader = new StreamReader(memoryStream);
            return memoryStreamReader.ReadToEnd();
        }

        /// <summary>
        /// Given an existing average value and existing count that contributed to that average,
        /// update the average based on a new count value and new average for that new count
        /// </summary>
        /// <param name="oldOverallCount"></param>
        /// <param name="oldOverallAverage"></param>
        /// <param name="newCount"></param>
        /// <param name="newAverage"></param>
        /// <returns>Updated average value</returns>
        private double UpdateAverage(int oldOverallCount, double oldOverallAverage, int newCount, double newAverage)
        {
            if (oldOverallCount == 0)
            {
                return newAverage;
            }

            if (newCount == 0)
            {
                return oldOverallAverage;
            }

            var oldCountTimesAverage = oldOverallCount * oldOverallAverage;
            var newCountTimesAverage = newCount * newAverage;

            var newOverallAverage = (oldCountTimesAverage + newCountTimesAverage) / (oldOverallCount + newCount);

            return newOverallAverage;
        }

        private string GetXmlValue(XmlNode xmlDoc, string xPath, string defaultValue)
        {
            var match = xmlDoc.SelectSingleNode(xPath);

            if (match == null)
            {
                return defaultValue;
            }

            return match.InnerText;
        }

        private int GetXmlValue(XmlNode xmlDoc, string xPath, int defaultValue)
        {
            var match = GetXmlValue(xmlDoc, xPath, defaultValue.ToString());

            if (int.TryParse(match, out var value))
            {
                return value;
            }

            return defaultValue;
        }

        private long GetXmlValueLong(XmlNode xmlDoc, string xPath, long defaultValue)
        {
            var match = GetXmlValue(xmlDoc, xPath, defaultValue.ToString(CultureInfo.InvariantCulture));

            if (long.TryParse(match, out var value))
            {
                return value;
            }

            return defaultValue;
        }

        private double GetXmlValue(XmlNode xmlDoc, string xPath, double defaultValue)
        {
            var match = GetXmlValue(xmlDoc, xPath, defaultValue.ToString(CultureInfo.InvariantCulture));

            if (double.TryParse(match, out var value))
            {
                return value;
            }

            return defaultValue;
        }
    }
}
