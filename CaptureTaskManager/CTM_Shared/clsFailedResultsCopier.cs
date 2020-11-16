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
        public void CopyFailedResultsToArchiveDirectory(string resultsDirectoryPath)
        {
            var failedResultsDirectoryPath = string.Empty;

            try
            {
                // Example path: D:\CTM_FailedResults
                failedResultsFolderPath = mMgrParams.GetParam("FailedResultsFolderPath");

                if (string.IsNullOrEmpty(failedResultsFolderPath))

                if (string.IsNullOrEmpty(failedResultsDirectoryPath))
                {
                    failedResultsDirectoryPath = mMgrParams.GetParam("FailedResultsFolderPath");
                }

                if (string.IsNullOrEmpty(failedResultsDirectoryPath))
                {
                    // Failed results folder directory is not defined; don't try to copy the results anywhere
                    LogWarning("FailedResultsDirectoryPath or FailedResultsFolderPath not defined for this manager; cannot copy results");
                    return;
                }

                var sourceDirectory = new DirectoryInfo(resultsDirectoryPath);
                var failedResultsDirectory = new DirectoryInfo(failedResultsDirectoryPath);

                // Make sure the failed results folder exists
                if (!failedResultsDirectory.Exists)
                {
                    failedResultsDirectory.Create();
                }

                // Define the target folder name to be Dataset_Job_Step

                var targetDirectoryPath = mTaskParams.GetParam("Dataset");
                if (string.IsNullOrEmpty(targetDirectoryPath))
                {
                    targetDirectoryPath = "Unknown_Dataset";
                }

                targetDirectoryPath += "_Job" + mTaskParams.GetParam("Job") + "_Step" + mTaskParams.GetParam("Step");

                targetDirectoryPath = Path.Combine(failedResultsDirectory.FullName, targetDirectoryPath);
                var targetDirectory = new DirectoryInfo(targetDirectoryPath);

                var infoFilePath = string.Empty;

                // Create an info file that describes the saved results
                try
                {
                    infoFilePath = Path.Combine(failedResultsDirectory.FullName,
                                                         FAILED_RESULTS_FOLDER_INFO_TEXT + targetDirectory.Name + ".txt");
                    CreateInfoFile(infoFilePath, targetDirectory.Name);
                }
                catch (Exception ex)
                {
                    LogError("Error creating the results folder info file '" + infoFilePath + "'", ex);
                }

                // Make sure the source folder exists
                if (!sourceDirectory.Exists)
                {
                    LogError("Source folder not found; cannot copy results: " + resultsDirectoryPath);
                }
                else
                {
                    // Look for failed results folders that were archived over FAILED_RESULTS_FOLDER_RETAIN_DAYS days ago
                    DeleteOldFailedResultsDirectories(failedResultsDirectory);

                    // Create the target folder
                    if (!targetDirectory.Exists)
                    {
                        targetDirectory.Create();
                    }

                    // Actually copy files from the source folder to the target folder
                    LogMessage("Copying data files to failed results archive: " + resultsDirectoryPath);

                    var errorCount = 0;
                    foreach (var sourceFile in sourceDirectory.GetFiles())
                    {
                        try
                        {
                            sourceFile.CopyTo(Path.Combine(targetDirectory.FullName, sourceFile.Name), true);
                        }
                        catch (Exception ex2)
                        {
                            LogError("Error copying file from " + resultsDirectoryPath + " to " + failedResultsDirectoryPath, ex2);
                            errorCount++;
                        }
                    }

                    if (errorCount == 0)
                    {
                        LogMessage("Copy complete");
                    }
                    else
                    {
                        LogWarning("Copy complete; ErrorCount = " + errorCount);
                    }
                }
            }
            catch (Exception ex)
            {
                LogError("Error copying results from " + resultsDirectoryPath + " to " + failedResultsDirectoryPath, ex);
            }
        }

        private void CreateInfoFile(string folderInfoFilePath, string resultsFolderName)
        {
            using (var infoFileWriter = new StreamWriter(new FileStream(folderInfoFilePath, FileMode.Create, FileAccess.Write, FileShare.Read)))
            {
                infoFileWriter.WriteLine("Date\t" + DateTime.Now);
                infoFileWriter.WriteLine("ResultsFolderName\t" + resultsFolderName);
                infoFileWriter.WriteLine("Manager\t" + mMgrParams.GetParam("MgrName"));
                if (mTaskParams != null)
                {
                    infoFileWriter.WriteLine("Job\t" + mTaskParams.GetParam("Job"));
                    infoFileWriter.WriteLine("Step\t" + mTaskParams.GetParam("Step"));
                    infoFileWriter.WriteLine("StepTool\t" + mTaskParams.GetParam("StepTool"));
                    infoFileWriter.WriteLine("Dataset\t" + mTaskParams.GetParam("Dataset"));
                }
                infoFileWriter.WriteLine("Date\t" + DateTime.Now);
            }
        }

        private void DeleteOldFailedResultsDirectories(DirectoryInfo failedResultsDirectory)
        {
            // Determine the folder archive time by reading the modification times on the ResultsFolderInfo_ files
            foreach (var resultFile in failedResultsDirectory.GetFiles(FAILED_RESULTS_FOLDER_INFO_TEXT + "*"))
            {
                if (DateTime.UtcNow.Subtract(resultFile.LastWriteTimeUtc).TotalDays <= FAILED_RESULTS_FOLDER_RETAIN_DAYS)
                {
                    continue;
                }

                // File was modified before the threshold; delete the results folder, then rename this file

                try
                {
                    var oldResultsDirectoryName = Path.GetFileNameWithoutExtension(resultFile.Name).Substring(FAILED_RESULTS_FOLDER_INFO_TEXT.Length);

                    if (resultFile.DirectoryName == null)
                    {
                        // Parent directory not defined; skip this folder
                        continue;
                    }

                    var oldResultsDirectory = new DirectoryInfo(Path.Combine(resultFile.DirectoryName, oldResultsDirectoryName));

                    if (oldResultsDirectory.Exists)
                    {
                        LogMessage("Deleting old failed results directory: " + oldResultsDirectory.FullName);

                        oldResultsDirectory.Delete(true);
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
