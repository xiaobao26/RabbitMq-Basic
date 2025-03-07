using System.Text;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;


var streamSystem = await StreamSystem.Create(new StreamSystemConfig());
var stream = "stream-offset-tracking-dotnet";
await streamSystem.CreateStream(new StreamSpec(stream));

// IOffsetType offsetSpecification = new OffsetTypeFirst();
IOffsetType offsetSpecification = new OffsetTypeNext();
ulong initialValue = UInt64.MaxValue;
ulong firstOffset = initialValue;
ulong lastOffset = initialValue;
var consumedCde = new CountdownEvent(1);
var consumer = await Consumer.Create(new ConsumerConfig(streamSystem, stream)
{
    OffsetSpec = offsetSpecification,
    MessageHandler = async (_, consumer, context, message) => {
        if (Interlocked.CompareExchange(ref firstOffset, context.Offset, initialValue) == initialValue) {
            Console.WriteLine("First message received.");
        }
        if ("marker".Equals(Encoding.UTF8.GetString(message.Data.Contents))) {
            Interlocked.Exchange(ref lastOffset, context.Offset);
            await consumer.Close();
            consumedCde.Signal();
        }
        await Task.CompletedTask;
    }
});
Console.WriteLine("Started consuming...");

consumedCde.Wait();
Console.WriteLine("Done consuming, first offset {0}, last offset {1}.", firstOffset, lastOffset);
await streamSystem.Close();