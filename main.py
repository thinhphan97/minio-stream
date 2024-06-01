from minio_stream import MinioStream, MIN_PART_SIZE


def read_in_chunks(file_object, chunk_size=MIN_PART_SIZE):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 5MB."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

client = MinioStream("play.min.io",
            access_key="#######################",
            secret_key="#######################",
            secure=False
            )

with client.open(bucket_name="stream-data", object_name="test.img") as up:
    with open('test.img', 'rb') as f:
        for piece in read_in_chunks(f):
            client.write(piece)

