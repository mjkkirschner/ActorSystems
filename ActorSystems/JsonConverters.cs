
using System.Reflection.PortableExecutable;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ActorSystems.JsonConverters {

        public class ObjectConverter : JsonConverter<object>
        {
            public override object Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
            return reader.TokenType switch
            {
                JsonTokenType.Number => reader.GetDouble(),
                JsonTokenType.String => reader.GetString(),
                JsonTokenType.True => reader.GetBoolean(),
                JsonTokenType.False => reader.GetBoolean(),
                JsonTokenType.StartArray => handleArray(ref reader, options),
                _ => throw new JsonException()
            };
            }
        private object[] handleArray(ref Utf8JsonReader reader, JsonSerializerOptions options)
        {
            var ar= JsonSerializer.Deserialize<object[]>(ref reader, options);
            reader.Read();

            return ar;
            
        }

            public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options)
            {
                throw new NotImplementedException();
            }
        }
}
