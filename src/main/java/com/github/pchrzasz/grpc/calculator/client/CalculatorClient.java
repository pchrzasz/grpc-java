package com.github.pchrzasz.grpc.calculator.client;


import com.proto.calculator.CalculatorServiceGrpc;
import com.proto.calculator.CalculatorServiceGrpc.CalculatorServiceBlockingStub;
import com.proto.calculator.SquareRootRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

/**
 * @author pchrzasz
 */
public class CalculatorClient {


	public static void main(String[] args) {
		System.out.println("Hello I'm a gRPC client");

		CalculatorClient main = new CalculatorClient();
		main.run();
	}

	private void run() {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50052)
				.usePlaintext()
				.build();

		doErrorCall(channel);

		System.out.println("Shutting down channel");
		channel.shutdown();

	}

	private void doErrorCall(ManagedChannel channel) {
		CalculatorServiceBlockingStub blockingStub = CalculatorServiceGrpc.newBlockingStub(channel);

		int number = -1;

		try {
			blockingStub.squareRoot(SquareRootRequest.newBuilder()
					.setNumber(number)
					.build());
		} catch (StatusRuntimeException e) {
			System.out.println("Got an exception for square root!");
			e.printStackTrace();
		}

	}
}
