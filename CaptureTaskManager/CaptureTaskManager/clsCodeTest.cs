using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CaptureTaskManager
{
	class clsCodeTest
	{
		//public void TestCalibrate()
		//{
		//    var m_DeMuxTool = new UIMFDemultiplexer.UIMFDemultiplexer();

		//    // Set the options
		//    m_DeMuxTool.ResumeDemultiplexing = false;
		//    m_DeMuxTool.CreateCheckpointFiles = false;

		//    // Set additional options
		//    m_DeMuxTool.MissingCalTableSearchExternal = true;       // Instruct tool to look for calibration table names in other similarly named .UIMF files if not found in the primary .UIMF file

		//    // Disable calibration if processing a .UIMF from the older IMS TOFs
		//    m_DeMuxTool.CalibrateAfterDemultiplexing = true;

		//    // Use all of the cores
		//    m_DeMuxTool.CPUCoresToUse = -1;

		//    bool success = m_DeMuxTool.CalibrateUIMFFile(@"\\proto-10\IMS04_AgTOF05\2011_2\Sarc_MS2_2_1Apr11_Cheetah_11-02-18\Sarc_MS2_2_1Apr11_Cheetah_11-02-18.uimf");

		//    if (success)
		//        Console.WriteLine("Success");
		//    else
		//        Console.WriteLine("Failed");
		//}

		public void TestConnection()
		{
			int iIterations = 0;

			Console.WriteLine("Code test mode");

			System.Collections.Generic.Dictionary<string, string> lstCredentials = new System.Collections.Generic.Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

			string sShareFolderPath = @"\\15T_FTICR.bionet\ProteomicsData";

			// Make sure sShareFolderPath does not end in a back slash
			if (sShareFolderPath.EndsWith(@"\"))
				sShareFolderPath = sShareFolderPath.Substring(0, sShareFolderPath.Length - 1);

			lstCredentials.Add("ftms", "PasswordHere");
			lstCredentials.Add("lcmsoperator", "PasswordHere");

			try
			{
				Dictionary<string, string>.Enumerator enumCurrent = lstCredentials.GetEnumerator();

				while (enumCurrent.MoveNext())
				{

					System.Net.NetworkCredential accessCredentials;
					accessCredentials = new System.Net.NetworkCredential(enumCurrent.Current.Key, enumCurrent.Current.Value, "");

					Console.WriteLine("Credentials created for " + enumCurrent.Current.Key);

					NetworkConnection cnBionet = new NetworkConnection(sShareFolderPath, accessCredentials);

					Console.WriteLine("Connected to share");

					System.IO.DirectoryInfo diDirectory = new System.IO.DirectoryInfo(sShareFolderPath);

					Console.WriteLine("Instantiated DirectoryInfo object: " + diDirectory.FullName);

					iIterations = 0;
					foreach (System.IO.FileInfo fiFile in diDirectory.GetFiles())
					{
						Console.WriteLine("File: " + fiFile.Name + " size " + fiFile.Length + " bytes");
						++iIterations;

						if (iIterations > 20)
							break;
					}

					Console.WriteLine("Files Done");

					iIterations = 0;
					foreach (System.IO.DirectoryInfo diFolder in diDirectory.GetDirectories())
					{
						Console.WriteLine("Folder: " + diFolder.Name + " modified " + diFolder.LastWriteTime.ToString());
						++iIterations;

						if (iIterations > 20)
							break;
					}

					Console.WriteLine("Folders Done");

					// Disconnect network connection
					cnBionet.Dispose();
					cnBionet = null;				

				}

			}
			catch (Exception ex)
			{
				Console.WriteLine("Exception: " + ex.Message);
			}

		}

	}
}
