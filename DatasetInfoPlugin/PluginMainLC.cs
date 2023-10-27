namespace DatasetInfoPlugin
{
    /// <summary>
    /// Dataset Info plugin: generates QC graphics using MSFileInfoScanner
    /// Overridden to set a flag for LC data capture
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    // ReSharper disable once InconsistentNaming
    public class PluginMainLC : PluginMain
    {
        /// <summary>
        /// Property to flag if this is being run specifically for LC data capture
        /// </summary>
        protected override bool IsLcDataCapture => true;
    }
}
