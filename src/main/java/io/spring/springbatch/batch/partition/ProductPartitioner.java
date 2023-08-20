package io.spring.springbatch.batch.partition;

import io.spring.springbatch.batch.domain.ProductVO;
import io.spring.springbatch.batch.job.api.QueryGenerator;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

public class ProductPartitioner implements Partitioner {

    private DataSource dataSource;

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        // 프로덕트의 타입 리스트 조회
        ProductVO[] productList = QueryGenerator.getProductList(dataSource);
        Map<String, ExecutionContext> result = new HashMap<>();

        int number = 0;

        for (int i = 0; i < productList.length; i++) {
            // 프로덕트 타입 별로 ExecutionContext를 생성 후 Map에 담는다.
            ExecutionContext value = new ExecutionContext();
            result.put("partition" + number, value);

            // 각 파티션 별로 타입이 담긴 ProductVO를 ExecutionContext에 담는다.
            value.put("product", productList[i]);

            number ++;
        }

        return result;
    }


}
