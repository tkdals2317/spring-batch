package io.spring.springbatch.batch.classifier;

import io.spring.springbatch.batch.domain.ApiRequestVO;
import io.spring.springbatch.batch.domain.ProductVO;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

import java.util.HashMap;
import java.util.Map;

public class WriteClassifier<C, T> implements Classifier<C, T> {

    private Map<String, ItemWriter<ApiRequestVO>> writerMap = new HashMap<>();

    @Override
    public T classify(C c) {
        return (T) writerMap.get(((ApiRequestVO)c).getProductVO().getType());
    }

    public void setWriterMap(Map<String, ItemWriter<ApiRequestVO>> writerMap) {
        this.writerMap = writerMap;
    }
}
