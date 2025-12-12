package ca.digilogue.xp.grpc;

import ca.digilogue.xp.App;
import ca.digilogue.xp.generator.OhlcvCandle;
import ca.digilogue.xp.generator.OhlcvGenerator;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for OHLCV candle data.
 * Provides access to real-time OHLCV candle data via gRPC.
 */
@GrpcService
public class OhlcvServiceImpl extends OhlcvServiceGrpc.OhlcvServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(OhlcvServiceImpl.class);

    @Override
    public void getLatestCandle(
            OhlcvServiceProto.GetLatestCandleRequest request,
            StreamObserver<OhlcvServiceProto.OhlcvCandleResponse> responseObserver) {

        String symbol = request.getSymbol();
        log.debug("Received request for latest candle: symbol={}", symbol);

        try {
            // Get the generator for the requested symbol
            OhlcvGenerator generator = App.getGenerator(symbol);

            if (generator == null) {
                log.warn("Generator not found for symbol: {}", symbol);
                responseObserver.onError(
                    io.grpc.Status.NOT_FOUND
                        .withDescription("No generator found for symbol: " + symbol)
                        .asRuntimeException()
                );
                return;
            }

            // Get the latest candle from the generator
            OhlcvCandle candle = generator.getLatestCandle();

            if (candle == null) {
                log.warn("No candle data available yet for symbol: {}", symbol);
                responseObserver.onError(
                    io.grpc.Status.NOT_FOUND
                        .withDescription("No candle data available yet for symbol: " + symbol)
                        .asRuntimeException()
                );
                return;
            }

            // Convert OhlcvCandle to protobuf response
            OhlcvServiceProto.OhlcvCandleResponse response = OhlcvServiceProto.OhlcvCandleResponse.newBuilder()
                .setSymbol(candle.getSymbol())
                .setOpen(candle.getOpen())
                .setHigh(candle.getHigh())
                .setLow(candle.getLow())
                .setClose(candle.getClose())
                .setVolume(candle.getVolume())
                .setTimestamp(convertInstantToNanos(candle.getTimestamp()))
                .build();

            log.debug("Sending response for symbol: {}, close={}", symbol, candle.getClose());
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("Error processing getLatestCandle request for symbol: {}", symbol, e);
            responseObserver.onError(
                io.grpc.Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException()
            );
        }
    }

    /**
     * Converts an Instant to nanoseconds since epoch.
     * 
     * @param instant The Instant to convert
     * @return Nanoseconds since epoch
     */
    private long convertInstantToNanos(java.time.Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }
}

