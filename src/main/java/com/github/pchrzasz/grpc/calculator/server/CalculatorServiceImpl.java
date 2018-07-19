package com.github.pchrzasz.grpc.calculator.server;

import com.proto.calculator.CalculatorServiceGrpc.CalculatorServiceImplBase;
import com.proto.calculator.SquareRootRequest;
import com.proto.calculator.SquareRootResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * @author pchrzasz
 */
public class CalculatorServiceImpl extends CalculatorServiceImplBase {

	@Override
	public void squareRoot(SquareRootRequest request, StreamObserver<SquareRootResponse> responseObserver) {

		Integer number = request.getNumber();

		if (number >= 0) {
			double numberRoot = Math.sqrt(number);
			responseObserver.onNext(
					SquareRootResponse.newBuilder()
							.setNumberRoot(numberRoot)
							.build()
			);
			responseObserver.onCompleted();
		} else {
			// we construct the exception
			responseObserver.onError(
					Status.INVALID_ARGUMENT
							.withDescription("The number being sent is not positive")
							.augmentDescription("Number sent: " + number)
							.asRuntimeException()
			);
		}


	}
}