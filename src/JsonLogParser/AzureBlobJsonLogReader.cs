using Azure.Storage.Blobs;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace JsonLogParser
{
    public class AzureBlobJsonLogReader : JsonLogReader
    {
        private BlobClient Blob { get; }

        private readonly long blobSize;

        private Stream baseStream;

        public AzureBlobJsonLogReader(Uri blobUri, IDictionary<string, string> fieldMapping, SourceJsonFormat format) : base(fieldMapping, format)
        {
            Blob = new BlobClient(blobUri);

            var properties = Blob.GetProperties();
            blobSize = properties.Value.ContentLength;
        }

        protected override long SourceStreamPosition => baseStream != null ? baseStream.Position : 0;

        protected override long Size => blobSize;

        protected override Stream OpenSourceStream()
        {
            var dl = Blob.Download();

            baseStream = dl.Value.Content;

            Stream blobStream = baseStream;

            return Blob.Name.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)
                ? new GZipStream(blobStream, CompressionMode.Decompress, false)
                : blobStream;
        }
    }
}
