using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;
using PRISM;

namespace ImsDemuxPlugin
{
    /// <summary>
    /// Tools for querying SQLite database (UIMF file, in this case)
    /// </summary>
    public class AgilentDotDTools : EventNotifier
    {
        // Ignore Spelling: demultiplexed, demultiplexing, demux, xs

        /// <summary>
        /// Evaluates the Agilent .D file to determine if it is multiplexed or not
        /// </summary>
        /// <param name="dotDFilePath">Full path to Agilent .D file</param>
        /// <param name="muxSequence">multiplexing encoding sequence; empty if not multiplexed</param>
        /// <returns>Enum indicating test results</returns>
        public MultiplexingStatus GetDotDMuxStatus(string dotDFilePath, out string muxSequence)
        {
            muxSequence = string.Empty;

            // Parse IMSFrameMeth.xml
            var methodInfo = ImsFrameMethMuxInfo(dotDFilePath);

            if (methodInfo == null || methodInfo.Count == 0)
            {
                return MultiplexingStatus.Error;
            }

            var status = MultiplexingStatus.NonMultiplexed;
            foreach (var method in methodInfo)
            {
                // Empty ImsMuxSequence string means not multiplexed, demux not required
                if (string.IsNullOrWhiteSpace(method.ImsMuxSequence))
                {
                    continue;
                }

                // ImsMuxProcessing != 1 means demux already occurred
                if (method.ImsMuxProcessing != 1)
                {
                    continue;
                }

                status = MultiplexingStatus.Multiplexed;

                if (string.IsNullOrWhiteSpace(muxSequence))
                {
                    muxSequence = method.ImsMuxSequence;
                }
                else if (!muxSequence.Equals(method.ImsMuxSequence))
                {
                    OnWarningEvent($"Multiple multiplexing sequences in file, which is abnormal: '{muxSequence}' and '{method.ImsMuxSequence}'");
                }
            }

            // Return results
            return status;
        }

        /// <summary>
        /// Evaluates the Agilent .D file to determine if it is already demultiplexed
        /// </summary>
        /// <param name="dotDFilePath">Full path to Agilent .D file</param>
        /// <returns>True if demultiplexed, false if not or error</returns>
        public bool GetDotDIsDemultiplexed(string dotDFilePath)
        {
            // Parse IMSFrameMeth.xml
            var methodInfo = ImsFrameMethMuxInfo(dotDFilePath);

            if (methodInfo == null || methodInfo.Count == 0)
            {
                return false;
            }

            foreach (var method in methodInfo)
            {
                // Empty ImsMuxSequence string means not multiplexed, demux not required
                if (string.IsNullOrWhiteSpace(method.ImsMuxSequence))
                {
                    continue;
                }

                // ImsMuxProcessing != 1 means demux already occurred
                if (method.ImsMuxProcessing == 1)
                {
                    return false;
                }
            }

            return true;
        }

        private List<(int ImsMuxProcessing, string ImsMuxSequence)> ImsFrameMethMuxInfo(string dotDFilePath)
        {
            // Agilent IMS multiplexing metadata, in IMSFrameMeth.xml:
            // ImsMuxProcessing:
            //   '1' for 'None' (either not multiplexed, or not demultiplexed)
            //   '2' for 'RealTime' (treat as not multiplexed, the data was already demultiplexed during acquisition)
            //   '3' for 'PostRun' (treat as not multiplexed, the data has already been demultiplexed)
            // ImsMuxSequence: sequence of '1's and '0's
            //   Not present for non-multiplexed files

            const string imsFrameMethSubPath = @"AcqData\IMSFrameMeth.xml";
            var imsFrameMethPath = Path.Combine(dotDFilePath, imsFrameMethSubPath);

            if (!File.Exists(imsFrameMethPath))
            {
                OnErrorEvent("Agilent .D file, IMSFrameMeth.xml does not exist: " + imsFrameMethSubPath);
                return null;
            }

            var methodInfo = new List<(int ImsMuxProcessing, string ImsMuxSequence)>();

            // Parse IMSFrameMeth.xml
            try
            {
                var document = new XmlDocument();
                document.Load(imsFrameMethPath);
                var manager = new XmlNamespaceManager(document.NameTable);
                manager.AddNamespace("xs", "http://www.w3.org/2001/XMLSchema");
                var query = "/FrameMethods/FrameMethod";
                var nodeList = document.SelectNodes(query, manager);
                foreach (XmlNode node in nodeList)
                {
                    // Parse each method separately
                    var methodMuxProcessing = 0; // bad value
                    var methodMuxSequence = "";
                    foreach (XmlNode node2 in node.ChildNodes)
                    {
                        switch (node2.Name)
                        {
                            case "ImsMuxProcessing":
                                methodMuxProcessing = int.Parse(node2.InnerXml);
                                break;
                            case "ImsMuxSequence":
                                methodMuxSequence = node2.InnerXml;
                                break;
                        }
                    }

                    if (methodMuxProcessing > 0)
                    {
                        methodInfo.Add(new(methodMuxProcessing, methodMuxSequence));
                    }
                }
            }
            catch (Exception)
            {
                OnErrorEvent("Agilent .D file, error parsing IMSFrameMeth.xml: " + imsFrameMethSubPath);
                return null;
            }

            if (methodInfo.Count == 0)
            {
                OnErrorEvent("Agilent .D file, error parsing IMSFrameMeth.xml - no ImsMuxProcessing entries: " + imsFrameMethSubPath);
            }

            return methodInfo;
        }
    }
}
