//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//*********************************************************************************************************

using PRISM;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using UIMFLibrary;

namespace ImsDemuxPlugin
{
    /// <summary>
    /// Tools for querying SQLite database (UIMF file, in this case)
    /// </summary>
    public class clsSQLiteTools : EventNotifier
    {

        public enum UimfQueryResults
        {
            NonMultiplexed,
            Multiplexed,
            Error
        }

        /// <summary>
        /// Evaluates the UIMF file to determine if it is multiplexed or not
        /// </summary>
        /// <param name="uimfFilePath">Full path to uimf file</param>
        /// <param name="numBitsForEncoding">Number of bits used for encoding; 0 if not multiplexed</param>
        /// <returns>Enum indicating test results</returns>
        public UimfQueryResults GetUimfMuxStatus(string uimfFilePath, out byte numBitsForEncoding)
        {
            numBitsForEncoding = 0;

            var encodingSequenceList = new SortedSet<string>();

            using (var reader = new DataReader(uimfFilePath))
            {
                var frameList = reader.GetMasterFrameList();

                foreach (var frame in frameList)
                {
                    var frameParams = reader.GetFrameParams(frame.Key);

                    var encodingSequence = frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence, string.Empty) ?? string.Empty;

                    if (!encodingSequenceList.Contains(encodingSequence))
                        encodingSequenceList.Add(encodingSequence);

                }
            }

            // If empty table returned, there was an error
            if (encodingSequenceList.Count < 1)
            {
                OnErrorEvent("UIMF file has no frames: " + uimfFilePath);
                return UimfQueryResults.Error;
            }

            var reBitValue = new Regex(@"^(\d)bit", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var imfProfileFieldAlwaysBlank = true;

            // Evaluate results. If any of the frames has a filename containing "bit", de-multiplexing is required
            // In addition, parse the "bit" value to determine numBitsForEncoding
            var deMuxRequired = false;
            foreach (var encodingSequence in encodingSequenceList)
            {

                // Empty string means demux not required
                if (string.IsNullOrWhiteSpace(encodingSequence))
                    continue;

                imfProfileFieldAlwaysBlank = false;

                // Get the file name, and check to see if it starts with 3bit (or 4bit or 5bit etc.)
                var multiplexFilename = Path.GetFileName(encodingSequence).ToLower();

                var reMatch = reBitValue.Match(multiplexFilename);
                if (!reMatch.Success)
                    continue;

                // Multiplex Filename contains "bit", so de-multiplexing is required
                deMuxRequired = true;

                byte.TryParse(reMatch.Groups[1].Value, out numBitsForEncoding);

                // No need to check additional rows; we assume the same multiplexing is used throughout the dataset
                break;
            }

            if (imfProfileFieldAlwaysBlank)
            {
                // Examine the filename to determine if it is a multiplexed dataset
                // This RegEx will match filenames of this format:
                //   BSA_65min_0pt5uL_1pt5ms_4bit_0001
                //   BSA_65min_0pt5uL_1pt5ms_4bit

                reBitValue = new Regex(@"(_(\d)bit_|_(\d)bit$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var fileName = Path.GetFileNameWithoutExtension(uimfFilePath);
                if (fileName != null)
                {
                    var reMatch = reBitValue.Match(fileName);
                    if (reMatch.Success)
                    {
                        // Filename contains "bit", so de-multiplexing is required
                        deMuxRequired = true;
                        byte.TryParse(reMatch.Groups[2].Value, out numBitsForEncoding);
                    }
                }
            }

            // Return results
            if (deMuxRequired)
            {
                return UimfQueryResults.Multiplexed;
            }

            return UimfQueryResults.NonMultiplexed;
        }

    }
}
