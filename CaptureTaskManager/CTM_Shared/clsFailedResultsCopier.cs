using System;
using System.IO;

namespace CaptureTaskManager
{
    // Used by ImsDemuxPlugin.clsDemuxTools
    // ReSharper disable once UnusedMember.Global
    public class clsFailedResultsCopier : clsLoggerBase
    {
        protected const string FAILED_RESULTS_FOLDER_INFO_TEXT = "FailedResultsFolderInfo_";
        protected const int FAILED_RESULTS_FOLDER_RETAIN_DAYS = 31;

        protected readonly IMgrParams mMgrParams;
        protected readonly ITaskParams mTaskParams;

        // Constructor
        public clsFailedResultsCopier(IMgrParams mgrParams, ITaskParams taskParams)
        {
            mMgrParams = mgrParams;
            mTaskParams = taskParams;
        }

        // ReSharper disable once UnusedMember.Global
        public void CopyFailedResultsToArchiveFolder(string resultsFolderPath)
        {
            var failedResultsFolderPath = string.Empty;

            try
            {
                // Example path: D:\CTM_FailedResults
                failedResultsFolderPath = mMgrParams.GetParam("FailedResultsFolderPath");

                if (string.IsNullOrEmpty(failedResultsFolderPath))
                {
                    // Failed results folder path is not defined; don't try to copy the results anywhere
                    LogWarning("FailedResultsFolderPath is not defined for this manager; cannot copy results");
                    return;
                }

                var diSourceFolder = new DirectoryInfo(resultsFolderPath);
                var diFailedResultsFolder = new DirectoryInfo(failedResultsFolderPath);

                // Make sure the failed results folder exists
                if (!diFailedResultsFolder.Exists)
                    diFailedResultsFolder.Create();

                // Define the target folder name to be Dataset_Job_Step

                var targetFolderPath = mTaskParams.GetParam("Dataset");
                if (string.IsNullOrEmpty(targetFolderPath))
                    targetFolderPath = "Unknown_Dataset";

                targetFolderPath += "_Job" + mTaskParams.GetParam("Job") + "_Step" + mTaskParams.GetParam("Step");

                targetFolderPath = Path.Combine(diFailedResultsFolder.FullName, targetFolderPath);
                var targetFolder = new DirectoryInfo(targetFolderPath);

                var folderInfoFilePath = string.Empty;

                // Create an info file that describes the saved results
                try
                {
                    folderInfoFilePath = Path.Combine(diFailedResultsFolder.FullName,
                                                         FAILED_RESULTS_FOLDER_INFO_TEXT + targetFolder.Name + ".txt");
                    CreateInfoFile(folderInfoFilePath, targetFolder.Name);
                }
                catch (Exception ex)
                {
                    LogError("Error creating the results folder info file '" + folderInfoFilePath + "'", ex);
                }

                // Make sure the source folder exists
                if (!diSourceFolder.Exists)
                {
                    LogError("Source folder not found; cannot copy results: " + resultsFolderPath);
                }
                else
                {
                    // Look for failed results folders that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                    DeleteOldFailedResultsFolders(diFailedResultsFolder);

                    // Create the target folder
                    if (!targetFolder.Exists)
                        targetFolder.Create();

                    // Actually copy files from the source folder to the target folder
                    LogMessage("Copying data files to failed results archive: " + resultsFolderPath);

                    var errorCount = 0;
                    foreach (var fiFileInfo in diSourceFolder.GetFiles())
                    {
                        try
                        {
                            fiFileInfo.CopyTo(Path.Combine(targetFolder.FullName, fiFileInfo.Name), true);
                        }
                        catch (Exception ex2)
                        {
                            LogError("Error copying file from " + resultsFolderPath + " to " + failedResultsFolderPath, ex2);
                            errorCount++;
                        }
                    }

                    if (errorCount == 0)
                        LogMessage("Copy complete");
                    else
                        LogWarning("Copy complete; ErrorCount = " + errorCount);
                }
            }
            catch (Exception ex)
            {
                LogError("Error copying results from " + resultsFolderPath + " to " + failedResultsFolderPath, ex);
            }
        }

        private void CreateInfoFile(string folderInfoFilePath, string resultsFolderName)
        {
            var swArchivedFolderInfoFile = new StreamWriter(new FileStream(folderInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read));

            {
                swArchivedFolderInfoFile.WriteLine("Date\t" + DateTime.Now);
                swArchivedFolderInfoFile.WriteLine("ResultsFolderName\t" + resultsFolderName);
                swArchivedFolderInfoFile.WriteLine("Manager\t" + mMgrParams.GetParam("MgrName"));
                if ((mTaskParams != null))
                {
                    swArchivedFolderInfoFile.WriteLine("Job\t" + mTaskParams.GetParam("Job"));
                    swArchivedFolderInfoFile.WriteLine("Step\t" + mTaskParams.GetParam("Step"));
                    swArchivedFolderInfoFile.WriteLine("StepTool\t" + mTaskParams.GetParam("StepTool"));
                    swArchivedFolderInfoFile.WriteLine("Dataset\t" + mTaskParams.GetParam("Dataset"));
                }
                swArchivedFolderInfoFile.WriteLine("Date\t" + DateTime.Now);
            }

            swArchivedFolderInfoFile.Close();
        }

        private void DeleteOldFailedResultsFolders(DirectoryInfo failedResultsFolder)
        {

            // Determine the folder archive time by reading the modification times on the ResultsFolderInfo_ files
            foreach (var resultFile in failedResultsFolder.GetFiles(FAILED_RESULTS_FOLDER_INFO_TEXT + "*"))
            {
                if (DateTime.UtcNow.Subtract(resultFile.LastWriteTimeUtc).TotalDays <= FAILED_RESULTS_FOLDER_RETAIN_DAYS)
                {
                    continue;
                }

                // File was modified before the threshold; delete the results folder, then rename this file

                try
                {
                    var oldResultsFolderName = Path.GetFileNameWithoutExtension(resultFile.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length);

                    if (resultFile.DirectoryName == null)
                    {
                        // Parent directory not defined; skip this folder
                        continue;
                    }

                    var diOldResultsFolder = new DirectoryInfo(Path.Combine(resultFile.DirectoryName, oldResultsFolderName));

                    if (diOldResultsFolder.Exists)
                    {
                        LogMessage("Deleting old failed results folder: " + diOldResultsFolder.FullName);

                        diOldResultsFolder.Delete(true);
                    }

                    var targetFilePath = string.Empty;

                    try
                    {
                        targetFilePath = Path.Combine(resultFile.DirectoryName, "x_" + resultFile.Name);
                        resultFile.CopyTo(targetFilePath, true);
                        resultFile.Delete();
                    }
                    catch (Exception ex)
                    {
                        LogError("Error renaming failed results info file to " + targetFilePath, ex);
                    }
                }
                catch (Exception ex)
                {
                    LogError("Error deleting old failed results folder", ex);
                }
            }
        }
    }
}
