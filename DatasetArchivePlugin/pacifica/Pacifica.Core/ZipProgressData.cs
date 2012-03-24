using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Pacifica.Core
{
    public class ZipProgressData
    {
        public string ArchiveName { get; set; }
        public long BytesTransferred { get; set; }
        public bool Cancel { get; set; }
        public int EntriesTotal { get; set; }
        public long TotalBytesToTransfer { get; set; }
    }
}
