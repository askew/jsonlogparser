using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

namespace JsonLogParser
{
    public abstract class JsonLogReader : IDisposable
    {
        private Stream inputStream;
        private const int ReadSize = 0x10_0000 * 10;

        /// <summary>Buffer A.</summary>
        private readonly byte[] _bufferA = new byte[ReadSize * 2];
        /// <summary>Buffer B.</summary>
        private readonly byte[] _bufferB = new byte[ReadSize * 2];

        /// <summary>Position of the start of data in the buffer.</summary>
        private int _bufferStart = ReadSize;
        /// <summary>Position of the end of data in the buffer.</summary>
        private int _bufferEnd = ReadSize;

        /// <summary>Position of the read head in the buffer</summary>
        private int _index = 0;

        bool aorb = true;

        /// <summary>The buffer that is being used to read from the stream.</summary>
        byte[] ReadBuffer => aorb ? _bufferA : _bufferB;

        /// <summary>The buffer that is being used to process the date from the previous read operation.</summary>
        byte[] ProcessBuffer => aorb ? _bufferB : _bufferA;

        // 
        public SourceJsonFormat JsonLinesFormat { get; }

        private IDictionary<string, string> PropertyMapping { get; } = new Dictionary<string, string>();

        public Action<int> Progress { get; set; }

        public JsonLogReader(IDictionary<string, string> fieldMapping) : this(fieldMapping, SourceJsonFormat.JsonLines)
        {
        }

        public JsonLogReader(IDictionary<string, string> fieldMapping, SourceJsonFormat format)
        {
            foreach (var key in fieldMapping.Keys)
            {
                PropertyMapping[key] = fieldMapping[key];
            }

            JsonLinesFormat = format;
        }

        protected abstract Stream OpenSourceStream();

        protected virtual long SourceStreamPosition => inputStream != null ? inputStream.Position : 0;
        protected virtual long Size => inputStream != null ? inputStream.Length : 0;

        public IEnumerable<string> GetRecords()
        {
            inputStream = OpenSourceStream();

            aorb = true;
            _bufferStart = ReadSize;

            // Start the first read. Note we always read from half-way to the end of the buffer.
            ValueTask<int> readtask = inputStream.ReadAsync(ReadBuffer.AsMemory(ReadSize, ReadSize));

            Console.CursorVisible = false;
            int pcentDone = 0;
            while (true)
            {
                // wait for the read to complete.
                int bytesRead = readtask.Result;

                // Switch buffers
                aorb = !aorb;

                // Start the next read. Note we always read from half-way to the end of the buffer.
                readtask = inputStream.ReadAsync(ReadBuffer.AsMemory(ReadSize, ReadSize));

                // Now process the completed read.
                string[] json = ProcessJsonData(bytesRead);

                if (Progress != null)
                {
                    int pcent = (int)Math.Round((double)SourceStreamPosition / (double)Size * 100d, 0);
                    if (pcent != pcentDone)
                    {
                        pcentDone = pcent;
                        Progress(pcent);
                    }
                }

                // If we didn't process any then assume we hit the end.
                if (json == null)
                {
                    break;
                }

                // Return the objects we have so far.
                foreach (string obj in json)
                {
                    yield return obj;
                }
            }
        }

        private readonly MemoryStream _outStream = new MemoryStream();
        private Utf8JsonWriter _jw;

        JsonReaderState _readerState;

        private string[] ProcessJsonData(int bytesRead)
        {
            if (bytesRead == 0)
            {
                return null;
            }
            _bufferEnd = ReadSize + bytesRead;

            List<string> objects = new List<string>();

            if (JsonLinesFormat == SourceJsonFormat.JsonLines)
            {
                foreach (ReadOnlyMemory<byte> line in GetLines())
                {
                    var rdr = new Utf8JsonReader(line.Span);
                    string jsonObject;
                    while ((jsonObject = ProcessJsonObject(ref rdr)) != null)
                    {
                        objects.Add(jsonObject);
                    }
                }
            }
            else
            {
                var rdr = new Utf8JsonReader(ProcessBuffer.AsSpan(_bufferStart, _bufferEnd - _bufferStart), false, _readerState);

                string jsonObject;
                while ((jsonObject = ProcessJsonObject(ref rdr)) != null)
                {
                    objects.Add(jsonObject);
                }

                _readerState = rdr.CurrentState;

                _index = (int)rdr.BytesConsumed;
            }

            // Copy the unprocessed bytes to the start of the other buffer.
            int bytesRemaining = _bufferEnd - _index;

            _bufferStart = ReadSize - bytesRemaining;
            ProcessBuffer.AsSpan(_index, bytesRemaining).CopyTo(ReadBuffer.AsSpan(_bufferStart));

            return objects.ToArray();
        }

        private IEnumerable<ReadOnlyMemory<byte>> GetLines()
        {
            int start = _bufferStart;
            _index = start;

            while (start < _bufferEnd && (ProcessBuffer[start] == '\n') || (ProcessBuffer[start] == '\r') || (ProcessBuffer[start] == ' ') || (ProcessBuffer[start] == '\t'))
            {
                start++;
            }

            for (int i = start; i < _bufferEnd; i++)
            {
                if (ProcessBuffer[i] == '\n')
                {
                    _index = i;
                    yield return new ReadOnlyMemory<byte>(ProcessBuffer, start, i - start);

                    i++;
                    while (i < _bufferEnd && (ProcessBuffer[i] == '\n') || (ProcessBuffer[i] == '\r') || (ProcessBuffer[i] == ' ') || (ProcessBuffer[i] == '\t'))
                    {
                        i++;
                    }
                    start = i;
                    continue;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private string ProcessJsonObject(ref Utf8JsonReader reader)
        {
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonTokenType.StartObject:
                        if (_jw != null)
                        {
                            _jw.Reset();
                        }
                        else
                        {
                            _jw = new Utf8JsonWriter(_outStream, new JsonWriterOptions { Indented = false });
                        }
                        _jw.WriteStartObject();

                        break;

                    case JsonTokenType.EndObject:
                        _jw.WriteEndObject();
                        _jw.Flush();

                        string jsonObj = Encoding.UTF8.GetString(_outStream.GetBuffer(), 0, (int)_outStream.Position);
                        _outStream.Position = 0L;
                        return jsonObj;

                    case JsonTokenType.PropertyName:
                        {
                            string key = reader.GetString();
                            if (PropertyMapping.ContainsKey(key))
                            {
                                _jw.WritePropertyName(PropertyMapping[key]);
                            }
                            else
                            {
                                _jw.WritePropertyName(key);
                            }
                            break;
                        }

                    case JsonTokenType.String:
                        {
                            string text = reader.GetString();
                            _jw.WriteStringValue(text);
                            break;
                        }

                    case JsonTokenType.Number:
                        {
                            double value = reader.GetDouble();
                            _jw.WriteNumberValue(value);
                            break;
                        }

                    case JsonTokenType.True:
                    case JsonTokenType.False:
                        {
                            _jw.WriteBooleanValue(reader.GetBoolean());
                            break;
                        }
                }
            }
            return null;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (inputStream != null)
                    {
                        inputStream.Dispose();
                    }
                }
                inputStream = null;

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion

    }
}
