package batcher

/*
func TestAzureSRStart_CreateContainerOnce(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName").
		WithSharedCapacity(10000, nil).
		WithFactor(1000)
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	//container.AssertNumberOfCalls(t, "Create", 1)
}

func TestAzureSRStart_CorrectNumberOfPartitionsCreated(t *testing.T) {
	container, blob := getMocks()
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(container, blob).
		WithFactor(1000)
	var wg sync.WaitGroup
	wg.Add(1)
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ProvisionDoneEvent:
			assert.Equal(t, 10, val)
			wg.Done()
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	blob.AssertNumberOfCalls(t, "Upload", 10)
}

func TestAzureSRStart_ContainerCanBeCreated(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var created int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case "created-container":
			created += 1
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	assert.Equal(t, 1, created, "expecting a single created event")
}

func TestAzureSRStart_ContainerCanBeVerified(t *testing.T) {
	var serr azblob.StorageError = StorageError{serviceCode: azblob.ServiceCodeContainerAlreadyExists}
	container := new(containerURLMock)
	container.On("Create", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, serr)
	_, blob := getMocks()
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(container, blob).
		WithFactor(1000)
	var verified int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case "verified-container":
			verified += 1
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	assert.Equal(t, 1, verified, "expecting a single verified event")
}

func TestAzureSRStart_BlobCanBeCreated(t *testing.T) {
	res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
		WithMocks(getMocks()).
		WithFactor(1000)
	var wg sync.WaitGroup
	wg.Add(1)
	var created int
	res.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.CreatedBlobEvent:
			created += 1
		case gobatcher.ProvisionDoneEvent:
			wg.Done()
		}
	})
	err := res.Start(context.Background())
	assert.NoError(t, err, "not expecting a start error")
	wg.Wait()
	assert.Equal(t, 10, created, "expecting a creation event per partition")
}

func TestAzureSRStart_BlobCanBeVerified(t *testing.T) {
	testCases := map[string]azblob.StorageError{
		"exists": StorageError{serviceCode: azblob.ServiceCodeBlobAlreadyExists},
		"leased": StorageError{serviceCode: azblob.ServiceCodeLeaseIDMissing},
	}
	for testName, serr := range testCases {
		t.Run(testName, func(t *testing.T) {
			container, _ := getMocks()
			blob := new(blockBlobURLMock)
			blob.On("Upload", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, serr)
			res := gobatcher.NewSharedResource("accountName", "containerName", 10000).
				WithMocks(container, blob).
				WithFactor(1000)
			var wg sync.WaitGroup
			wg.Add(1)
			var verified int
			res.AddListener(func(event string, val int, msg string, metadata interface{}) {
				switch event {
				case gobatcher.VerifiedBlobEvent:
					verified += 1
				case gobatcher.ProvisionDoneEvent:
					wg.Done()
				}
			})
			err := res.Start(context.Background())
			assert.NoError(t, err, "not expecting a start error")
			wg.Wait()
			assert.Equal(t, 10, verified, "expecting a verified event per partition")
		})
	}
}

*/
