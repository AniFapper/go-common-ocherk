package uow

import "context"

// UnitOfWork defines a generic interface for managing database transactions.
// T represents the "Store" or "Repository Registry" — a collection of repositories
// that should share the same transaction context.
//
// Example implementation of T:
//
//	type AuthStore interface {
//	    Users() UserRepository
//	    OAuth() OAuthRepository
//	    RefreshTokens() RefreshTokenRepository
//	}
//
// Usage example in a UseCase:
//
//	err := uc.uow.Do(ctx, func(ctx context.Context, store repository.AuthStore) error {
//	    user, err := store.Users().Create(ctx, newUser)
//	    if err != nil {
//	        return err // Transaction will be rolled back automatically
//	    }
//
//	    oauth.UserID = user.ID
//	    return store.OAuth().Create(ctx, oauth) // If nil is returned, transaction commits
//	})
type UnitOfWork[T any] interface {
	// Do executes the provided function within a single transaction scope.
	// If the function returns an error, the transaction is rolled back.
	// If the function returns nil, the transaction is committed.
	Do(ctx context.Context, fn func(ctx context.Context, store T) error) error
}
