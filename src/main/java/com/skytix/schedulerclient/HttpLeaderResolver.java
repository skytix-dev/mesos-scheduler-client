package com.skytix.schedulerclient;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;

@Slf4j
public class HttpLeaderResolver implements LeaderResolver {
    private final String mMesosPath;
    private final HttpClient mHttpClient;

    public HttpLeaderResolver(String aMesosPath, HttpClient aHttpClient) {
        mMesosPath = aMesosPath;
        mHttpClient = aHttpClient;
    }

    @Override
    public String resolveLeader() throws NoLeaderException {

        try {
            final URI uri = new URI(String.format("%s/redirect", mMesosPath));

            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET()
                    .build();

            final HttpResponse<String> response = mHttpClient.send(request, HttpResponse.BodyHandlers.ofString());

            switch (response.statusCode()) {

                case 307:
                    final Optional<String> location = response.headers().firstValue("Location");

                    if (location.isPresent()) {
                        final String url = String.format("%s:%s", uri.getScheme(), location.get());

                        log.info(String.format("Discovered Mesos master node at: %s", url));
                        return url;

                    } else {
                        throw new NoLeaderException("Unable to find master in missing Location header");
                    }

                case 503:
                    throw new NoLeaderException("An elected Mesos master node cannot be found");

                default:
                    throw new NoLeaderException("Unable to determine the current leader");
            }

        } catch (URISyntaxException | IOException | InterruptedException aE) {
            throw new NoLeaderException(aE);
        }

    }

}
