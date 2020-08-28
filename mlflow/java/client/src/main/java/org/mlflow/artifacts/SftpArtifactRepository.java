package org.mlflow.artifacts;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.jcraft.jsch.*;
import org.mlflow.api.proto.Service;
import org.mlflow.tracking.creds.MlflowHostCredsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.awt.HKSCS;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.function.Function;

public class SftpArtifactRepository implements ArtifactRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(CliBasedArtifactRepository.class);
    private static final Set<String> EXCLUDED_FILES = Sets.newHashSet(".", "..");

    // Base directory of the artifactory, used to let the user know why this repository was chosen.
    private final URI targetUri;

    // Run ID this repository is targeting.
    private final String runId;

    private final JSch jschClient;

    // Used to pass credentials as environment variables
    // (e.g., MLFLOW_TRACKING_URI or DATABRICKS_HOST) to the mlflow process.
    private final MlflowHostCredsProvider hostCredsProvider;

    public SftpArtifactRepository(URI targetUri, String runId, MlflowHostCredsProvider hostCredsProvider) {
        this.targetUri = targetUri;
        this.runId = runId;
        this.hostCredsProvider = hostCredsProvider;
        this.jschClient = new JSch();
    }

    @Override
    public void logArtifact(File localFile) {
        logArtifact(localFile, null);
    }

    @Override
    public void logArtifact(File localFile, String artifactPath) {

        try {
            useSftpChannel(sftpChannel -> {
                copyFile(sftpChannel, localFile.toPath(), artifactPath);
                return null;
            });

        } catch (JSchException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void logArtifacts(File localDir) {
        logArtifacts(localDir, null);
    }

    @Override
    public void logArtifacts(File localDir, String artifactPath) {
        try {
            useSftpChannel(sftpChannel -> {
                try {
                    Files.list(localDir.toPath()) //
                            .filter(Files::isRegularFile) //
                            .forEach(localFile -> {
                                copyFile(sftpChannel, localFile, artifactPath);
                            });
                    return null;
                } catch (IOException e) {
                    throw new RuntimeException("Error while trying to get files from " + localDir
                            + " (to put on Sftp server " + targetUri.getHost() + ")", e);
                }
            });

        } catch (JSchException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Service.FileInfo> listArtifacts() {
        return listArtifacts(null);
    }

    @Override
    public List<Service.FileInfo> listArtifacts(String artifactPath) {
        try {
            return useSftpChannel(sftpChannel -> {
                try {

                    final List<Service.FileInfo> result = Lists.newArrayList();

                    ChannelSftp.LsEntrySelector addToResultsSelector = lsEntry -> {
                        if (! EXCLUDED_FILES.contains(lsEntry.getFilename()))
                        result.add(convertToFileInfo(lsEntry));
                        return ChannelSftp.LsEntrySelector.CONTINUE;
                    };

                    sftpChannel.ls(createTargetFolderLocation(artifactPath).toString(), addToResultsSelector);

                    return result;

                } catch (SftpException e) {
                    throw new RuntimeException("Error while listing artifacts (artifactPath: " + artifactPath + ")", e);
                }
            });
        } catch (JSchException e) {
            throw new RuntimeException("Error while listing artifacts (artifactPath: " + artifactPath + ")", e);
        }
    }

    @Override
    public File downloadArtifacts() {
        return downloadArtifacts(null);
    }

    @Override
    public File downloadArtifacts(String artifactPath) {
        try {
            return useSftpChannel(sftpChannel -> {
                try {

                    // Returns a file when pointing to a file...
                    // Returns a directory containing all remote files when pointing to a directory...
                    // artifactPath can be a file or directory!

                    File tempFile = File.createTempFile("mlflow.", ".sftp.obj");
                    sftpChannel.get(cr);
                    ChannelSftp.LsEntrySelector addToResultsSelector = lsEntry -> {
                        if (! EXCLUDED_FILES.contains(lsEntry.getFilename()))
                            result.add(convertToFileInfo(lsEntry));
                        return ChannelSftp.LsEntrySelector.CONTINUE;
                    };

                    sftpChannel.ls(createTargetFolderLocation(artifactPath).toString(), addToResultsSelector);

                    return result;

                } catch (SftpException | IOException e) {
                    throw new RuntimeException("Error while listing artifacts (artifactPath: " + artifactPath + ")", e);
                }
            });
        } catch (JSchException e) {
            throw new RuntimeException("Error while listing artifacts (artifactPath: " + artifactPath + ")", e);
        }
    }

    private Service.FileInfo convertToFileInfo(ChannelSftp.LsEntry lsEntry) {
        return Service.FileInfo.newBuilder()
                .setFileSize(lsEntry.getAttrs().getSize())
                .setIsDir(lsEntry.getAttrs().isDir())
                .setPath(lsEntry.getFilename())
                .build();
    }

    private <T> T useSftpChannel(Function<ChannelSftp, T> consumer) throws JSchException {
        Session session = null;
        ChannelSftp sftpChannel = null;
        try {
            session = createJschSession();
            sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();

            return consumer.apply(sftpChannel);

        } finally {

            if (sftpChannel != null) {
                try {
                    sftpChannel.disconnect();
                } catch (Exception e) {
                    LOGGER.warn("Error while trying to close SFTP Channel. " +
                            "Ignoring it, but now trying to close the SSH session itself.", e);
                }
            }

            if (session != null) {
                session.disconnect();
            }

        }
    }

    private Session createJschSession() throws JSchException {
        UriUserInfo userInfo = new UriUserInfo(targetUri.getUserInfo());

        LOGGER.debug("Going to connect to {}:{} with login {}",
                targetUri.getHost(), targetUri.getPort(), userInfo.getUsername());

        Session session = jschClient.getSession(userInfo.getUsername(), targetUri.getHost());
        userInfo.getPassword().ifPresent(session::setPassword);
        session.setPort(targetUri.getPort());

        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);

        session.connect();

        return session;
    }

    private void copyFile(ChannelSftp channelSftp, Path localFile, String artifactPath) {
        Path targetLocation = createTargetFileLocation(localFile.toFile(), artifactPath);
        try {
            if (null != artifactPath && !remoteFileExists(channelSftp, targetLocation.getParent())) {
                channelSftp.mkdir(targetLocation.getParent().toString());
            }
            channelSftp.put(localFile.toString(), targetLocation.toString());
        } catch (SftpException e) {
            throw new RuntimeException("Error while copying file '" + localFile
                    + "' to sftp on host '" + this.targetUri.getHost() + "' under " + targetLocation, e);
        }
    }

    private boolean remoteFileExists(ChannelSftp channelSftp, Path parent) {
        try {
            channelSftp.stat(parent.toString());
            return true;
        } catch (SftpException e) {
            return false;
        }
    }

    private Path createTargetFileLocation(File localFile, String artifactPath) {
        return createTargetFolderLocation(artifactPath).resolve(localFile.getName());
    }

    private Path createTargetFolderLocation(String artifactPath) {
        if (null == artifactPath) {
            return Paths.get(targetUri.getPath());
        } else {
            return Paths.get(targetUri.getPath()).resolve(artifactPath);
        }
    }

}
