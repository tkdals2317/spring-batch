package io.spring.springbatch.batch.chunk.processor;

import io.spring.springbatch.batch.domain.ApiRequestVO;
import io.spring.springbatch.batch.domain.ProductVO;
import org.springframework.batch.item.ItemProcessor;

public class ApiItemProcessor2 implements ItemProcessor<ProductVO, ApiRequestVO> {
    @Override
    public ApiRequestVO process(ProductVO productVO) throws Exception {
        return ApiRequestVO.builder()
                .id(productVO.getId())
                .productVO(productVO)
                .build();
    }
}
