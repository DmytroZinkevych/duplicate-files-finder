import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DuplicateFilesFinder {

    record FileData(
            Path path,
            long size,
            String checksum
    ) { }

    public static void main(String[] args) {
        String sourceDir = args.length == 0
                ? "." + File.separator
                : args[0];
        String duplicatesDir = sourceDir + File.separator + "duplicates";

        try (Stream<Path> stream = Files.list(Path.of(sourceDir))) {
            Files.createDirectories(Paths.get(duplicatesDir));
            AtomicInteger movedAmount = new AtomicInteger();
            stream
                    .filter(Files::isRegularFile)
                    .map(DuplicateFilesFinder::toFileData)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.groupingBy(FileData::checksum))
                    .values()
                    .stream()
                    .filter(fileDataList -> fileDataList.size() > 1)
                    .flatMap(Collection::stream)
                    .forEach(fileData -> {
                        try {
                            Path checksumDir = Paths.get(duplicatesDir + File.separator + fileData.checksum);
                            Files.createDirectories(checksumDir);
                            Path newFilePath = checksumDir.resolve(fileData.path.getFileName());
                            Files.move(fileData.path, newFilePath);
                            movedAmount.incrementAndGet();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
            System.out.println("Successfully moved " + movedAmount + " files");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Scanner s = new Scanner(System.in);
        System.out.println("Press enter to exit . . .");
        s.nextLine();
    }

    private static Optional<FileData> toFileData(Path path) {
        try {
            byte[] data = Files.readAllBytes(path);
            byte[] hash = MessageDigest.getInstance("MD5").digest(data);
            String checksum = new BigInteger(1, hash).toString(16);
            return Optional.of(
                    new FileData(
                            path,
                            path.toFile().length(),
                            checksum
                    )
            );
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
