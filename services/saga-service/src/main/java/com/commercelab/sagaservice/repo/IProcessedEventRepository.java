package com.commercelab.sagaservice.repo;

import com.commercelab.sagaservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface IProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
}
