using System.Collections.Generic;

namespace Pacifica.Core
{
    public interface IUpload
    {
        /// <summary>
        /// Receives an dictionary object with appropriate name-value pairs of metadata information
        /// </summary>
        /// <param name="metadataObject"></param>
        /// <param name="statusURL"></param>
        /// <remarks>Raises event with URL of status monitor page from server backend</remarks>
        bool StartUpload(List<Dictionary<string, object>> metadataObject, out string statusURL);

        /// <summary>
        /// Generates a SHA-1 style hash of a given file
        /// </summary>
        /// <param name="fileName">Path to the file</param>
        /// <returns>Returns Hex-encoded hash string</returns>
        string GenerateSha1Hash(string fileName);

        event StatusUpdateEventHandler StatusUpdate;
        event UploadCompletedEventHandler UploadCompleted;

    }
}