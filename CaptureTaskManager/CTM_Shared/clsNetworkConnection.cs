using System;
using System.ComponentModel;
using System.Net;
using System.Runtime.InteropServices;

// ReSharper disable once CommentTypo
/*
 * Code written by Luke Quinane and posted to
 * http://stackoverflow.com/questions/295538/how-to-provide-user-name-and-password-when-connecting-to-a-network-share
 *
 */

namespace CaptureTaskManager
{
    public sealed class NetworkConnection : IDisposable
    {
        readonly string _networkName;

        public NetworkConnection(string networkName,
                                 NetworkCredential credentials)
        {
            _networkName = networkName;

            var netResource = new NetResource()
            {
                Scope = ResourceScope.GlobalNetwork,
                ResourceType = ResourceType.Disk,
                DisplayType = ResourceDisplayType.Share,
                RemoteName = networkName
            };

            var result = WNetAddConnection2(
                netResource,
                credentials.Password,
                credentials.UserName,
                0);

            if (result != 0)
            {
                throw new Win32Exception(result);
            }
        }

        ~NetworkConnection()
        {
            DisposeNetConnection();
        }

        public void Dispose()
        {
            DisposeNetConnection();
            GC.SuppressFinalize(this);
        }

        private void DisposeNetConnection()
        {
            WNetCancelConnection2(_networkName, 0, true);
        }

        [DllImport("mpr.dll")]
        private static extern int WNetAddConnection2(NetResource netResource,
                                                     string password, string username, int flags);

        [DllImport("mpr.dll")]
        private static extern int WNetCancelConnection2(string name, int flags,
                                                        bool force);
    }

    // ReSharper disable UnusedMember.Global

    [StructLayout(LayoutKind.Sequential)]
    public class NetResource
    {
        public ResourceScope Scope;
        public ResourceType ResourceType;
        public ResourceDisplayType DisplayType;
        public int Usage;
        public string LocalName;
        public string RemoteName;
        public string Comment;
        public string Provider;
    }

    public enum ResourceScope
    {
        Connected = 1,
        GlobalNetwork,
        Remembered,
        Recent,
        Context
    };

    public enum ResourceType
    {
        Any = 0,
        Disk = 1,
        Print = 2,
        Reserved = 8,
    }

    public enum ResourceDisplayType
    {
        Generic = 0x0,
        Domain = 0x01,
        Server = 0x02,
        Share = 0x03,
        File = 0x04,
        Group = 0x05,
        Network = 0x06,
        Root = 0x07,
        ShareAdmin = 0x08,
        Directory = 0x09,
        Tree = 0x0a,
        NdsContainer = 0x0b
    }

    // ReSharper restore UnusedMember.Global

}