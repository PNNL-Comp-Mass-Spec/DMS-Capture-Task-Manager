using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CaptureToolPlugin
{
    /// <summary>
    /// Dataset capture plugin, overridden to set a flag for LC data capture
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
