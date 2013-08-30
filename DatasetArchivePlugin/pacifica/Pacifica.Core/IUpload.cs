using System.Collections;
using System.Net;

namespace Pacifica.Core
{
	public interface IUpload
	{
		/// <summary>
		/// Receives an IDictionary-style object with appropriate name-value pairs of metadata information
		/// </summary>
		/// <param name="MetadataObject"></param>
		/// <remarks>Raises event with URL of status monitor page from server backend</remarks>
		bool StartUpload(IDictionary metadataObject, out string statusURL);

		/// <summary>
		/// Receives an IDictionary-style object with appropriate name-value pairs of metadata information
		/// </summary>
		/// <param name="MetadataObject"></param>
		/// <param name="LoginCredentials"></param>
		/// <remarks>Raises event with URL of status monitor page from server backend</remarks>
		bool StartUpload(IDictionary metadataObject, NetworkCredential loginCredentials, out string statusURL);

		/// <summary>
		/// Generates a SHA-1 style hash of a given file
		/// </summary>
		/// <param name="FullFilePath">Path to the file</param>
		/// <returns>Returns Hex-encoded hash string</returns>
		string GenerateSha1Hash(string fileName);

		/// <summary>
		/// Returns status for the file in question from the server
		/// </summary>
		/// <remarks>Works off of an independent background process that talks
		/// to an URL from the backend server</remarks>
		event MessageEventHandler DebugEvent;
		event MessageEventHandler ErrorEvent;
		event StatusUpdateEventHandler StatusUpdate;
		event UploadCompletedEventHandler UploadCompleted;

	}
}