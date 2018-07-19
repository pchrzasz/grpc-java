package com.github.pchrzasz.grpc.greeting.server;

import com.proto.greet.*;
import com.proto.greet.GreetServiceGrpc.GreetServiceImplBase;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

/**
 * @author pchrzasz
 */
class GreetServiceImpl extends GreetServiceImplBase {

	// Unary
	@Override
	public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
		// extract fields we need
		Greeting greeting = request.getGreeting();
		String firstName = greeting.getFirstName();

		// create response
		String result = "Hello " + firstName;
		GreetResponse response = GreetResponse.newBuilder().setResult(result).build();

		// send response
		responseObserver.onNext(response);

		// complete the RPC call
		responseObserver.onCompleted();
	}

	// Server Streaming
	@Override
	public void greetManyTimes(GreetManyTimesRequest request, StreamObserver<GreetManyTimesResponse> responseObserver) {
		String firstName = request.getGreeting().getFirstName();

		try {
			for (int i = 0; i < 10; i++) {
				String result = "Hello " + firstName + ", response number: " + i;
				GreetManyTimesResponse response = GreetManyTimesResponse.newBuilder().setResult(result).build();

				responseObserver.onNext(response);
				Thread.sleep(1000L);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			responseObserver.onCompleted();
		}
	}

	// Client Streaming
	@Override
	public StreamObserver<LongGreetRequest> longGreet(StreamObserver<LongGreetResponse> responseObserver) {
		StreamObserver<LongGreetRequest> requestObserver = new StreamObserver<LongGreetRequest>() {

			StringBuilder stringBuilder = new StringBuilder();

			@Override
			public void onNext(LongGreetRequest longGreetRequest) {
				// client sends a message
				stringBuilder.append("Hello ")
						.append(longGreetRequest.getGreeting().getFirstName())
						.append(" ! ");
			}

			@Override
			public void onError(Throwable throwable) {
				// client sends an error
			}

			@Override
			public void onCompleted() {
				// client is done
				responseObserver.onNext(
						LongGreetResponse.newBuilder()
								.setResult(stringBuilder.toString())
								.build()
				);
				responseObserver.onCompleted();
			}
		};
		return requestObserver;
	}

	@Override
	public StreamObserver<GreetEveryoneRequest> greetEveryone(StreamObserver<GreetEveryoneResponse> responseObserver) {
		StreamObserver<GreetEveryoneRequest> requestObserver = new StreamObserver<GreetEveryoneRequest>() {
			@Override
			public void onNext(GreetEveryoneRequest greetEveryoneRequest) {
				String result = "Hello " + greetEveryoneRequest.getGreeting().getFirstName();
				GreetEveryoneResponse greetEveryoneResponse = GreetEveryoneResponse.newBuilder()
						.setResult(result).build();

				responseObserver.onNext(greetEveryoneResponse);
			}

			@Override
			public void onError(Throwable throwable) {

			}

			@Override
			public void onCompleted() {
				responseObserver.onCompleted();
			}
		};
		return requestObserver;
	}

	@Override
	public void greetWithDeadline(GreetWithDeadlineRequest request, StreamObserver<GreetWithDeadlineResponse> responseObserver) {

		Context current = Context.current();

		try {
			for (int i = 0; i < 3; i++) {
				if (!current.isCancelled()) {
					System.out.println("sleep for 100 ms");
					Thread.sleep(100);
				} else {
					return;
				}
			}

			System.out.println("send response");
			responseObserver.onNext(
					GreetWithDeadlineResponse.newBuilder()
							.setResult("hello " + request.getGreeting().getFirstName())
							.build()
			);

			responseObserver.onCompleted();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
