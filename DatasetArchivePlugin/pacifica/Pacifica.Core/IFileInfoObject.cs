using System;
using System.Collections;

namespace Pacifica.Core
{
    public interface IFileInfoObject
    {
        string AbsoluteLocalPath { get; }
        string RelativeDestinationDirectory { get; }
        string RelativeDestinationFullPath { get; }
        string FileName { get; }
        string Sha1HashHex { get; }
        long FileSizeInBytes { get; }
        DateTime CreationTime { get; }
        DateTime SubmittedTime { get; }
        string CreationTimeStamp { get; }
        string SubmittedTimeStamp { get; }
        string SerializeData();
        IDictionary SerializeToDictionaryObject();
    }
}