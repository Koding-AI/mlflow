package org.mlflow.artifacts;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.junit.rules.TemporaryFolder;
import org.mlflow.api.proto.Service;
import org.mlflow.tracking.creds.MlflowHostCredsProvider;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.org.apache.commons.lang.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

public class SftpArtifactRepositoryTest {

    private static final Logger logger = LoggerFactory.getLogger(SftpArtifactRepositoryTest.class);

    private static final int SSH_PORT = 22;

    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "password";
    private static final String CONTAINER_PATH = "upload";
    private static final String CONTAINER_PATH_IN_CONTAINER = "/home/" + USERNAME + "/";

    private static TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static GenericContainer atmozSftpContainer = new GenericContainer("atmoz/sftp:latest") //
            .withExposedPorts(SSH_PORT) //
            .withCommand(String.format("%s:%s:::%s", USERNAME, PASSWORD, CONTAINER_PATH));

    private SftpArtifactRepository client;

    @BeforeClass
    public void startContainer() throws Exception {
        atmozSftpContainer.start();
        temporaryFolder.create();
        MlflowHostCredsProvider hostCredsProvider = Mockito.mock(MlflowHostCredsProvider.class);
        int port = atmozSftpContainer.getMappedPort(SSH_PORT);
        URI uri = new URI(String.format("sftp://%s:%s@localhost:%d/%s", USERNAME, PASSWORD, port, CONTAINER_PATH));
        client = new SftpArtifactRepository(uri, "run-id", hostCredsProvider);
    }

    @AfterClass
    public void stopContainer() {
        atmozSftpContainer.stop();
        temporaryFolder.delete();
    }

    @BeforeSuite
    public void beforeAll() throws Exception {
//        MlflowHostCredsProvider hostCredsProvider = Mockito.mock(MlflowHostCredsProvider.class);
//        int port = atmozSftpContainer.getMappedPort(22);
//        URI uri = new URI(String.format("sftp://%s:%s@localhost:%d/%s", USERNAME, PASSWORD, port, CONTAINER_PATH));
//        client = new SftpArtifactRepository(uri, "run-id", hostCredsProvider);
    }


    @Test
    public void logArtifact() throws Exception {
        File tempFile = createTempFile("logArtifact");
        client.logArtifact(tempFile);

        assertContainerFileExists(CONTAINER_PATH + "/" + tempFile.getName(), "logArtifact");
    }

    @Test
    public void logArtifactWithPath() throws Exception {

        File tempFile = createTempFile("logArtifact:somewhere");
        client.logArtifact(tempFile, "somewhere");

        assertContainerFileExists(CONTAINER_PATH + "/somewhere/" + tempFile.getName(), "logArtifact:somewhere");

    }

    @Test
    public void logArtifacts() throws Exception {

        File tempDirectory = temporaryFolder.newFolder();
        createTempFile(tempDirectory, "log.first.txt", "logArtifacts1");
        createTempFile(tempDirectory, "log.second.txt", "logArtifacts2");

        client.logArtifacts(tempDirectory);

        assertContainerFileExists(CONTAINER_PATH + "/log.first.txt", "logArtifacts1");
        assertContainerFileExists(CONTAINER_PATH + "/log.second.txt", "logArtifacts2");

    }


    @Test
    public void logArtifactsWithPath() throws Exception {
        File tempDirectory = temporaryFolder.newFolder();
        createTempFile(tempDirectory, "log.first.txt", "logArtifacts1:somewhere");
        createTempFile(tempDirectory, "log.second.txt", "logArtifacts2:somewhere");

        client.logArtifacts(tempDirectory, "somewhere");

        assertContainerFileExists(CONTAINER_PATH + "/somewhere/log.first.txt", "logArtifacts1:somewhere");
        assertContainerFileExists(CONTAINER_PATH + "/somewhere/log.second.txt", "logArtifacts2:somewhere");

    }

    @Test
    public void listArtifacts() {

        // First upload some...
        putFileInContainer("/home/" + USERNAME + "/" + CONTAINER_PATH + "/list.first.txt", "first");
        putFileInContainer("/home/" + USERNAME + "/" + CONTAINER_PATH + "/list.second.txt", "second");

        // Then query them
        List<Service.FileInfo> result = client.listArtifacts();
        sortByFileName(result);

        logger.info("Result: {}", result);

        // And check whether everything was found, and nothing more
        assertEquals(2, result.size());


        assertEquals("list.first.txt", result.get(0).getPath());
        assertEquals("first".length(), result.get(0).getFileSize());

        assertEquals("list.second.txt", result.get(1).getPath());
        assertEquals("second".length(), result.get(1).getFileSize());
    }

    private void sortByFileName(List<Service.FileInfo> result) {
        Collections.sort(result, Comparator.comparing(Service.FileInfo::getPath));
    }

    @Test
    public void listArtifactsWithPath() {
        // First upload some...
        putFileInContainer("/home/" + USERNAME + "/" + CONTAINER_PATH + "/sub/list.first.txt", "first");
        putFileInContainer("/home/" + USERNAME + "/" + CONTAINER_PATH + "/sub/list.second.txt", "second");
        putFileInContainer("/home/" + USERNAME + "/" + CONTAINER_PATH + "/list.third.txt", "third");

        // Then query them
        List<Service.FileInfo> result = client.listArtifacts("sub");
        sortByFileName(result);

        logger.info("Result: {}", result);

        // And check whether everything was found, and nothing more
        assertEquals(2, result.size());

        assertEquals("list.first.txt", result.get(0).getPath());
        assertEquals("first".length(), result.get(0).getFileSize());

        assertEquals("list.second.txt", result.get(1).getPath());
        assertEquals("second".length(), result.get(1).getFileSize());

    }

    @Test
    public void downloadArtifacts() throws Exception {
        // Put file in container
        putFileInContainer("/home/" + USERNAME + "/" + CONTAINER_PATH + "/download.first.txt", "first");

        // Retrieve it
        File result = client.downloadArtifacts();

        // Check whether it's ok
        assertEquals("first", FileUtils.readFileToString(result, StandardCharsets.UTF_8));
    }

    @Test
    public void downloadArtifacts(String artifactPath) throws Exception {
        // Put file in container
        putFileInContainer("/home/" + USERNAME + "/" + CONTAINER_PATH + "/sub/download.first.txt", "first");

        // Retrieve it
        File result = client.downloadArtifacts("sub");

        // Check whether it's ok
        assertEquals("first", FileUtils.readFileToString(result, StandardCharsets.UTF_8));
    }

    private void assertContainerFileExists(String expectedFile, String expectedContent) throws Exception {
        File targetFile = temporaryFolder.newFolder().toPath().resolve("targetfile").toFile();
        atmozSftpContainer.copyFileFromContainer(CONTAINER_PATH_IN_CONTAINER + expectedFile, targetFile.getAbsolutePath());
        assertTrue("File " + expectedFile + " must exist in container", targetFile.exists());
        String content = FileUtils.readFileToString(targetFile, "UTF-8");
        assertTrue("File " + expectedFile + " must contain '" + expectedContent + "'. Content is: " + StringUtils.abbreviate(expectedContent, 500), content.contains(expectedContent));
    }

    private File createTempFile(String content) throws IOException {
        File file = temporaryFolder.newFile();
        FileUtils.write(file, content, "UTF-8");
        return file;
    }

    private File createTempFile(File parentDirectory, String filename, String content) throws IOException {
        File file = parentDirectory.toPath().resolve(filename).toFile();
        FileUtils.write(file, content, "UTF-8");
        return file;
    }

    private void putFileInContainer(String fileLocation, String content) {
        atmozSftpContainer.copyFileToContainer(Transferable.of(content.getBytes(StandardCharsets.UTF_8)), fileLocation);
    }

}
