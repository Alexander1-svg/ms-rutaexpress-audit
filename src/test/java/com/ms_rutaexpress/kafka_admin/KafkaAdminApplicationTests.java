package com.ms_rutaexpress.kafka_admin;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class KafkaAdminApplicationTests {

	@MockitoBean
	private Admin admin;

	@Test
	void contextLoads() {
	}

}
