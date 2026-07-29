package com.tesis.plataforma;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.ObjectMapper;

@SpringBootApplication
public class PlataformaApplication {

	public static void main(String[] args) {
		SpringApplication.run(PlataformaApplication.class, args);
	}

}
