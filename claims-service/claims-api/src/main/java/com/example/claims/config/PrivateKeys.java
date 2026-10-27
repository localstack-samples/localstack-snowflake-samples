package com.example.claims.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Loads an unencrypted PKCS#8 PEM private key ({@code -----BEGIN PRIVATE KEY-----}),
 * the format {@code openssl genpkey} writes.
 *
 * <p>The key is handed to the JDBC driver as a {@link PrivateKey} object rather than a
 * file path. That keeps the driver's key-file permission checks out of the picture
 * (a key on a shared or mounted volume is often group-readable) and works the same
 * whether the key came from a file, a secret manager or a test fixture.
 */
public final class PrivateKeys {

    private static final String BEGIN = "-----BEGIN PRIVATE KEY-----";
    private static final String END = "-----END PRIVATE KEY-----";

    private PrivateKeys() {
    }

    public static PrivateKey readPkcs8Pem(Path path) {
        String pem;
        try {
            pem = Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read Snowflake private key " + path, e);
        }
        int begin = pem.indexOf(BEGIN);
        int end = pem.indexOf(END);
        if (begin < 0 || end < begin) {
            throw new IllegalArgumentException(path + " is not an unencrypted PKCS#8 PEM key ("
                    + BEGIN + "). Generate one with: openssl genpkey -algorithm RSA "
                    + "-pkeyopt rsa_keygen_bits:2048 -out rsa_key.p8");
        }
        String base64 = pem.substring(begin + BEGIN.length(), end).replaceAll("\\s", "");
        try {
            return KeyFactory.getInstance("RSA")
                    .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException(path + " does not contain a valid RSA private key", e);
        }
    }
}
