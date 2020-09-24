namespace Pacifica.Core
{
    public delegate void MessageEventHandler(object sender, MessageEventArgs e);

    public delegate void StatusUpdateEventHandler(object sender, StatusEventArgs e);

    public delegate void UploadCompletedEventHandler(object sender, UploadCompletedEventArgs e);
}