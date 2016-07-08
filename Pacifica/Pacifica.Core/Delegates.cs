namespace Pacifica.Core
{
    public delegate void DebugEventHandler(object sender, MessageEventArgs e);
    public delegate void MessageEventHandler(object sender, MessageEventArgs e);

    public delegate void ProgressEventHandler(object sender, ProgressEventArgs e);
    public delegate void StatusUpdateEventHandler(object sender, StatusEventArgs e);

    public delegate void UploadCompletedEventHandler(object sender, UploadCompletedEventArgs e);

}